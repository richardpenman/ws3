__doc__ = 'High level functions for interpreting useful data from input'

import csv, logging, math, os, random, re
from . import common


def extract_emails(html, ignored=None):
    """Remove common obfuscations from HTML and then extract all emails

    ignored: 
        list of dummy emails to ignore

    >>> extract_emails('')
    []
    >>> extract_emails('hello contact@webscraping.com world')
    ['contact@webscraping.com']
    >>> extract_emails('hello contact@<!-- trick comment -->webscraping.com world')
    ['contact@webscraping.com']
    >>> extract_emails('hello contact AT webscraping DOT com world')
    ['contact@webscraping.com']
    >>> extract_emails(' info+hn@gmail.com ')
    ['info+hn@gmail.com']
    >>> extract_emails('<a href="mailto:first.last@mail.co.uk">Contact</a>')
    ['first.last@mail.co.uk']
    """
    emails = []
    if html:
        email_re = re.compile('([\w\.\-\+]{1,64})@(\w[\w\.-]{1,255})\.(\w+)')
        # remove comments, which can obfuscate emails
        html = re.compile('<!--.*?-->', re.DOTALL).sub('', html).replace('mailto:', '')
        for user, domain, ext in email_re.findall(html):
            if ext.lower() not in common.MEDIA_EXTENSIONS and len(ext)>=2 and not re.compile('\d').search(ext) and domain.count('.')<=3:
                email = '%s@%s.%s' % (user, domain, ext)
                if email not in emails:
                    emails.append(email)

        # look for obfuscated email
        for user, domain, ext in re.compile('([\w\.\-\+]{1,64})\s?.?AT.?\s?([\w\.-]{1,255})\s?.?DOT.?\s?(\w+)', re.IGNORECASE).findall(html):
            if ext.lower() not in common.MEDIA_EXTENSIONS and len(ext)>=2 and not re.compile('\d').search(ext) and domain.count('.')<=3:
                email = '%s@%s.%s' % (user, domain, ext)
                if email not in emails:
                    emails.append(email)
    if ignored:
        emails = [email for email in emails if email not in ignored]
    return emails


def extract_phones(html):
    """Extract phone numbers from this HTML

    >>> extract_phones('Phone: (123) 456-7890 <br>')
    ['(123) 456-7890']
    >>> extract_phones('Phone 123.456.7890 ')
    ['123.456.7890']
    >>> extract_phones('+1-123-456-7890<br />123 456 7890n')
    ['123-456-7890', '123 456 7890']
    >>> extract_phones('456-7890')
    []
    >>> extract_phones('<a href="tel:0234673460">Contact</a>')
    ['0234673460']
    """
    return [match.group() for match in re.finditer('(\+\d{1,2}\s)?\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4}', html)] + re.findall('tel:(\d+)', html)


def parse_us_address(address):
    """Parse USA address into address, city, state, and zip code

    >>> parse_us_address('6200 20th Street, Vero Beach, FL 32966')
    ('6200 20th Street', 'Vero Beach', 'FL', '32966')
    """
    city = state = zipcode = ''
    addrs = map(lambda x:x.strip(), address.split(','))
    if addrs:
        m = re.compile('([A-Z]{2,})\s*(\d[\d\-\s]+\d)').search(addrs[-1])
        if m:
            state = m.groups()[0].strip()
            zipcode = m.groups()[1].strip()

            if len(addrs)>=3:
                city = addrs[-2].strip()
                address = ','.join(addrs[:-2])
            else:
                address = ','.join(addrs[:-1])
            
    return address, city, state, zipcode


def get_earth_radius(scale):
    if scale is None:
        return 1.0
    elif scale == 'km':
        return 6373.0
    elif scale == 'miles':
        return 3960.0
    else:
        raise common.WebScrapingError('Invalid scale: %s' % str(scale))


def distance(p1, p2, scale=None):
    """Calculate distance between 2 (latitude, longitude) points.

    scale:
        By default the distance will be returned as a ratio of the earth's radius
        Use 'km' to return distance in kilometres, 'miles' to return distance in miles

    >>> melbourne = -37.7833, 144.9667
    >>> san_francisco = 37.7750, -122.4183
    >>> int(distance(melbourne, san_francisco, 'km'))
    12659
    """
    if p1 == p2:
        return 0
    lat1, long1 = p1
    lat2, long2 = p2
    # Convert latitude and longitude to 
    # spherical coordinates in radians.
    degrees_to_radians = math.pi / 180.0
        
    # phi = 90 - latitude
    phi1 = (90.0 - lat1)*degrees_to_radians
    phi2 = (90.0 - lat2)*degrees_to_radians
        
    # theta = longitude
    theta1 = long1*degrees_to_radians
    theta2 = long2*degrees_to_radians
        
    cos = (math.sin(phi1)*math.sin(phi2)*math.cos(theta1 - theta2) + math.cos(phi1)*math.cos(phi2))
    arc = math.acos(cos)
    return arc * get_earth_radius(scale)


def find_coordinates(ch_lat=100, ch_lng=100, ch_scale='miles', min_lat=-90, max_lat=90, min_lng=-180, max_lng=180):
    """Find all latitude/longitude coordinates within bounding box, with given increments
    """
    cur_lat = min_lat
    while cur_lat < max_lat:
        cur_lng = min_lng
        while cur_lng < max_lng:
            yield cur_lat, cur_lng
            _, cur_lng = move_coordinate(cur_lat, cur_lng, 0, ch_lng, ch_scale)
        cur_lat, _ = move_coordinate(cur_lat, cur_lng, ch_lat, 0, ch_scale)


def move_coordinate(lat, lng, ch_lat, ch_lng, ch_scale=None):
    """Move latitude/longitude coordinate a given increment
    """
    r_earth = get_earth_radius(ch_scale)
    new_lat = lat + (ch_lat / r_earth) * (180 / math.pi);
    new_lng = lng + (ch_lng / r_earth) * (180 / math.pi) / math.cos(lat * math.pi/180.0)
    return new_lat, new_lng


def get_zip_codes(filename, min_distance=100, scale='miles', lat_key='Latitude', lng_key='Longitude', zip_key='Zip'):
    """Reads CSV file of zip,lat,lng and returns zip codes that aren't within the minimum distance of each other
    """
    for zip_code, lat, lng in get_zip_lat_lngs(filename, min_distance, scale, lat_key, lng_key, zip_key):
        yield zip_code

def get_zip_lat_lngs(filename, min_distance=100, scale='miles', lat_key='Latitude', lng_key='Longitude', zip_key='Zip'):
    locations = []
    for record in csv.DictReader(open(filename)):
        lat, lng = float(record[lat_key]), float(record[lng_key])
        for other_lat, other_lng in locations:
            if distance((lat, lng), (other_lat, other_lng), scale=scale) < min_distance:
                break
        else:
            locations.append((lat, lng))
            yield record[zip_key], record[lat_key], record[lng_key]


def find_json_path(e, value, path=''):
    """Find the JSON path that points to this value
    """
    results = []
    if e == value:
        results.append(path)
    if isinstance(e, dict):
        for k, v in e.items():
            key_path = '{}["{}"]'.format(path, k)
            results.extend(find_json_path(v, value, key_path))
    elif isinstance(e, list):
        for i, v in enumerate(e):
            index_path = '{}[{}]'.format(path, i)
            results.extend(find_json_path(v, value, index_path))
    return results
