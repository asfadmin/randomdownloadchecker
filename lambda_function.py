import base64
import json
import os
import random
import re
import time
import traceback
import uuid
from http import cookiejar
from urllib import request
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import boto3

# Download read chunk size
READ_CHUNK_SIZE = 16 * 1024 * 1024

# CMR URLS
cmr_api = os.getenv('cmr_api')
cmr_coll_url = '{0}/search/collections.json?{1}&page_size=1000'.format(cmr_api, os.getenv('collection_filter'))
cmr_gran_cnt_url = '{0}/search/granules?page_size=1&collection_concept_id='.format(cmr_api)
cmr_gran_url = '{0}/search/granules.json?page_size=5&collection_concept_id='.format(cmr_api)
# opener class to prevent redirects (we'll redirect manually.)


class NoRedirect(request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


# Keep 'em cookies!
cj = cookiejar.CookieJar()
opener = request.build_opener(request.HTTPCookieProcessor(cj), NoRedirect)
request.install_opener(opener)


def get_uuid():
    return str(uuid.uuid1()).lower()


def get_creds():
    username = os.getenv('urs_user')
    password = os.getenv('urs_pass')
    u_p = bytes(f"{username}:{password}", 'utf-8')
    return base64.b64encode(u_p).decode('utf-8')


# Download an object payload
def simple_download(url):
    try:
        resp = urlopen(Request(url))
        return resp.read().decode('utf-8')
    except Exception as E:
        print(f"Could not hit {url}: {E}")
        raise E


# for same-host redirects
def add_url_host(url, new_url):
    return "https://{0}{1}".format(url.split('/')[2], new_url)


def trace_obj(new_url, e, timer):
    return {'url': new_url, 'code': e.code, 'duration': (time.time() - timer) * 1000}


# Empty the buffer into the trash.
def read_file_to_devnull(resp):
    dl_size = 0
    with open('/dev/null', 'wb') as f:
        while True:
            chunk = resp.read(READ_CHUNK_SIZE)
            if not chunk:
                break
            dl_size += len(chunk)
            f.write(chunk)
    return dl_size


# Recursively follow redirects
def make_request(url, origin_request=None, trace=None):
    trace = trace or []

    # Watch out for redirect loops
    if len(trace) > 6:
        print(f"TOO MANY REDIRECTS: {trace}")
        return trace, False

    # We can use this to trace our requests in TEA
    headers = {'x-origin-request-id': origin_request} if origin_request else {}
    # Only send Auth Creds to EDL
    if "urs.earthdata.nasa.gov/oauth/authorize" in url:
        print(".... + Adding basic auth headers")
        headers['Authorization'] = "Basic {0}".format(get_creds())

    # Start the request timer
    timer = time.time()
    req = Request(url, headers=headers)
    try:
        resp = urlopen(req)

    except HTTPError as e:
        if e.code == 401 and e.getheader('Location'):
            # Password failed or other unknown auth problem...
            print(f"Redirecting for auth: {e.getheader('Location')}")
            trace.append(trace_obj(url, e, timer))
            return trace, False

        elif e.code >= 300 and e.code <= 400:
            # Redirect response....
            new_url = e.getheader('Location')

            # Check for self-redirects
            if 'https://' not in new_url:
                new_url = add_url_host(url, new_url)

            # Recursively Follow redirect
            print(f" .... Redirecting w/ {e.code} to {new_url}")
            trace.append(trace_obj(url, e, timer))

            return make_request(new_url, origin_request, trace)
        else:
            # Some other failure... 404?
            trace.append(trace_obj(url, e, timer))
            print(f"Hit HTTPError....{e}")
            return trace, False

    except Exception as E:
        # DNS problem?
        print(f"Could not hit {url}: {E}")
        return trace, False

    # Dump the response data to /dev/null & stop the clock
    dl_size = read_file_to_devnull(resp)
    dl_time = (time.time() - timer) * 1000

    # Grab the content-length header
    object_size = int(resp.getheader('content-length'))
    print(f"Downloaded {dl_size} of {object_size} in {dl_time}ms ")
    trace.append({"url": url, "code": resp.code, "duration": dl_time, "size": dl_size})

    # Make sure we were able to read the whole file...
    if object_size != dl_size:
        print("We did not download the whole file.... ")
        return trace, False

    # Everything worked!
    return trace, True


def send_sns(message, subject='Downoad Test Failure'):
    client = boto3.client('sns')
    client.publish(TopicArn=os.getenv('sns_arn'), Message=message, Subject=subject)


def get_grans_from_collection(collection, cnt):
    gran_set = []

    for _ in (1, 2):
        # Pick 2 random pages of 5 granules from the collection
        page_num = random.randint(1, min(int(cnt / 5), 1000))
        gran_url = cmr_gran_url + "{0}&page_num={1}".format(collection, page_num)

        # Grab the granule metadata as JSON
        gran_json = json.loads(simple_download(gran_url))

        # Make sure data has "granule_size" attribute
        incomplete_records = len([d for d in gran_json['feed']['entry'] if 'granule_size' not in d])

        if incomplete_records:
            print("Found {0} records without granule_size in collection {1}".format(incomplete_records, collection))

        # Randomly select 1 href from the links
        for rangran in [d for d in gran_json['feed']['entry'] if 'granule_size' in d and float(d['granule_size']) <= 300]:

            # Ignore links records from json object that have '(VIEW RELATED INFORMATION)', they come from OnlineResources
            hrefs = [l['href'] for l in rangran['links'] if ('inherited' not in l and l['rel'] == 'http://esipfed.org/ns/fedsearch/1.1/data#')]
            if not hrefs:
                print("... No downloads in {0}".format(rangran))
            random_file = random.choice(hrefs)

            # Make sure we don't do the same file twice!
            if random_file not in gran_set:
                # This is our download!
                print("... adding {0} from {1} in {2} ...".format(random_file, rangran['id'], collection))
                gran_set.append(random_file)

    return gran_set


def get_cmr_granules():

    # query CMR for collections, then grab the URLS from some random granules
    granule_url_set = []

    print("... Gathering 200+ random granules ....")
    collections = json.loads(simple_download(cmr_coll_url))
    collections = [d['id'] for d in collections['feed']['entry']]
    random.shuffle(collections)

    #  Loop over randomized collections
    for collection in collections:
        # Skip Dynamic Products, they trigger deglaciation or have no size param
        if collection in os.getenv('skip_collections').split(','):
            print('Skipping dynamic collection: {0}'.format(collection))
            continue

        # grab the hit count for the collection
        coll_count_xml = simple_download(cmr_gran_cnt_url + collection)
        cnt = int(re.findall(r'\<hits\>(\d+)\<\/hits\>', coll_count_xml)[0])

        # Only sample large collections
        if cnt > 10000:
            print("   ... collection {0} has {1} granule ... ".format(collection, cnt))
            granule_url_set += get_grans_from_collection(collection, cnt)

        # stop after we've found 200 "random granules"
        if len(granule_url_set) >= 200:
            print("... Found enough granules... ")
            break

    # Choose 20 of the random 200!
    products = random.choices(granule_url_set, k=20)
    print("... Selected the following granules: {0}".format(products))
    return products


# Format the trace
def format_trace(tb):
    tb_rpt = ""
    for cnt, stop in enumerate(tb):
        short_url = stop['url'].split('?')[0]
        tb_rpt += f"  +{'-'*(cnt+1)}> {short_url} w/ {stop['code']} for {stop['duration']}\n"
    return tb_rpt


def summarize_everything(good, bad, origin_request):
    final_report = ""

    if bad:
        final_report += f"{len(bad)} of {(len(good) + len(bad))} downloads failed...\n"
    else:
        final_report += "All Downloads Successful!\n"
    final_report += "\nGood Downloads:\n"

    for g in good:
        size_mb = g['dl_size'] / (1024 * 1024)
        rate = size_mb / (g['work_time'] / 1000)
        final_report += f"  - {g['url']}: {size_mb:.1f}MB @ {rate:.01f}MB/sec (overhead: {g['overhead']:.01f}ms)\n"

    if bad:
        final_report += "\nFailed Downloads:\n"
    for b in bad:
        final_report += f"  - {b['url']} failed w/ {b['code']} (overhead: {b['overhead']:.01f}ms)\n"
        # If we have a redirect, add the trace
        if len(b['tb']) > 1:
            final_report += format_trace(b['tb']) + "\n"

    # This value can be used to search the TEA logs
    final_report += f"\nx-origin-request-id was {origin_request}\n"
    return final_report


def lambda_handler(_event, _context):
    # For tracking this run in TEA logs.
    origin_request = get_uuid()

    # keep track of whats working
    (good, bad) = ([], [])

    try:
        # Get 20 random downloads to try
        random_downloads = get_cmr_granules()

        for url in random_downloads:
            print(f"Downloading {url}...")

            timer = time.time()
            tb, ok = make_request(url, origin_request)
            duration = (time.time() - timer) * 1000

            if ok:
                work_time = tb[-1]['duration']
                dl_size = tb[-1]['size']
                overhead = duration - work_time
                print(f"Successfully download {url}: spent {work_time}ms downloading {dl_size}b (overhead: {overhead})")
                good.append({"url": url, "work_time": work_time, "dl_size": dl_size, "overhead": overhead, "tb": tb})

            else:
                code = tb[-1]["code"] if tb else None
                bad.append({"url": url, "overhead": duration, "tb": tb, "code": code})
                print(f"{url} was NOT successful... spent {duration}ms, result was {code}")

        summary = summarize_everything(good, bad, origin_request)

        if bad:
            send_sns(message=summary, subject="Failure!")
        else:
            send_sns(message=summary, subject="Success!")
            return True

    except Exception as E:
        print("problem running code: {0}".format(E))
        send_sns(message=f"There was a problem running download report: {traceback.format_exc()}", subject="Error!")
        raise (E)

    return False
