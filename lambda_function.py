import urllib.parse, urllib.request
import os, sys, io, re, json, random, tempfile
from contextlib import redirect_stdout
import boto3

# from download script
#pylint: disable=unused-import
import base64, time, ssl, xml.etree.ElementTree as ET, shutil
from urllib.request import build_opener, install_opener, Request, urlopen
from urllib.request import HTTPHandler, HTTPSHandler, HTTPCookieProcessor
from urllib.error import HTTPError, URLError
from http.cookiejar import MozillaCookieJar
#pylint: enable=unused-import

def get_url(req_url):
    try:
        resp = urllib.request.urlopen( req_url ).read().decode('utf-8')
        return resp
    except urllib.error.HTTPError as E:
        raise ValueError('HTTPError downloading {0}, code:{1}, reason:{2}\n\nResponse Payload (If any):\n{3}'.format(req_url, E.code, E.reason, E.read().decode('utf-8') )) 
    except urllib.error.URLError as E:
        raise ValueError('URLError downloading {0}, reason:{1}; Error: {2}'.format( req_url, E.reason, E ))
    except Exception as E:
        raise ValueError('Problem downloading {0}: {1}'.format( req_url, E ))


def send_sns(message, subject='Downoad Test Failure'):
    client = boto3.client('sns')
    client.publish(TopicArn=os.getenv('sns_arn'),Message=message,Subject=subject)

def lambda_handler(event, context): 				#pylint: disable=unused-argument
    cmr_api = os.getenv('cmr_api')
    cmr_coll_url = '{0}/search/collections.json?{1}&page_size=1000'.format(cmr_api,os.getenv('collection_filter'))
    cmr_gran_cnt_url = '{0}/search/granules?page_size=1&collection_concept_id='.format(cmr_api)
    cmr_gran_url = '{0}/search/granules.json?page_size=5&collection_concept_id='.format(cmr_api)

    try:
    
        # query CMR for collections, then grab the URLS from some random granules
        granule_url_set = []
        print ("... Gathering 200+ random granules ....")
        collections = json.loads(get_url ( cmr_coll_url ) )
        collections = [d['id'] for d in collections['feed']['entry']]
        random.shuffle(collections)

        #  Loop over randomized collections
        for collection in collections:
            # Skip Dynamic Products, they trigger deglaciation or have no size param
            if collection in os.getenv('skip_collections').split(','):
                print('Skipping dynamic collection: {0}'.format(collection))
                continue

            # grab the hit count for the collection
            coll_count_xml = get_url ( cmr_gran_cnt_url+collection )
            cnt = int(re.findall(r'\<hits\>(\d+)\<\/hits\>', coll_count_xml)[0])

            # Only sample large collections
            if cnt > 10000:
                print ("   ... collection {0} has {1} granule ... ".format(collection, cnt))
                for _ in [1,2]:
                    # Pick 2 random pages of 5 granules from the collection
                    page_num = random.randint(1, int(cnt/5))
                    gran_url = cmr_gran_url + "{0}&page_num={1}".format(collection, page_num)

                    # Grab the granule metadata as JSON
                    gran_json = json.loads(get_url(gran_url))

                    # Make sure data has "granule_size" attribute
                    incomplete_records = len([d for d in gran_json['feed']['entry'] if 'granule_size' not in d])
                    if incomplete_records:
                        print ("Found {0} records without granule_size in collection {1}".format(incomplete_records, collection))

                    # Randomly select 1 href from the links
                    for rangran in [d for d in gran_json['feed']['entry'] if 'granule_size' in d and float(d['granule_size']) <= 300 ]:
                        
                        # Ignore links records from json object that have '(VIEW RELATED INFORMATION)', they come from OnlineResources
                        hrefs = [l['href'] for l in rangran['links'] if ( 'inherited' not in l and l['rel'] == 'http://esipfed.org/ns/fedsearch/1.1/data#') ]
                        if not hrefs:
                            print("... No downloads in {0}".format(rangran))
                        random_file = random.choice(hrefs)
                        
                        # Make sure we don't do the same file twice!
                        if random_file not in granule_url_set:
                            # This is our download!
                            print("... adding {0} from {1} in {2} ...".format(random_file, rangran['id'], collection ))
                            granule_url_set.append(random_file)

            # stop after we've found 200 "random granules"
            if len ( granule_url_set ) >= 200:
                print ( "... Found enough granules... ")
                break

        # Choose 20 of the random 200!
        products = random.choices( granule_url_set, k=20)
        print ("... Selected the following granules: {0}".format(products))

        # Fake U:P Info
        print ("... Faking input.... ")
        username = os.getenv('urs_user')
        password = os.getenv('urs_pass')
        sys.stdin = io.StringIO("{0}\n{1}\n".format(username, password))

        # Download Bulk Download SCirpt
        url = 'https://bulk-download.asf.alaska.edu/?products='
        print ("... Getting download script ... ")
        products_encoded = urllib.parse.quote(",".join(products))
        code = get_url ( url+products_encoded )

        #### Change various pieces of the download script to work in lambda
        # Change input for U:
        code = re.sub( r'raw_input\(\"Username.*\)', 'sys.stdin.readline().rstrip()', code)
        code = re.sub( r'getpass\.getpass\(.*\"\)', 'sys.stdin.readline().rstrip()', code)
        # Fix bad vertex link, this should be fixed eventually. ASF specific download token.
        code = re.sub( r'vertex\.daac\.asf\.alaska\.edu', 'vertex-retired.daac.asf.alaska.edu', code)
        # Trick code into running automagically
        code = re.sub( r'if __name__ \=\= \"__main__\"\:', 'if True:', code)
        # Use CWD instead of home
        code = re.sub( r'os\.path\.expanduser\(\'\~\'\)', 'os.getcwd()', code )
        # Prevent temp file copy:
        code = re.sub( r'shutil\.copy\(tempfile_name\, download_file\)', 'download_file=tempfile_name', code)
        code = re.sub( r'os\.remove\(tempfile_name\)', '#os.remove(tempfile_name)', code)
        # Clean up downloads
        code = re.sub( r'if file_size is None', "os.remove(download_file)\n       if file_size is None", code)
        # Fix a potential 401 bug?
        code = re.sub( r'with open\(tempfile_name\, \'r\'\) as myfile', "try:\n                 tempfile_name\n             except NameError:\n                 return False,None\n             with open(tempfile_name, 'r') as myfile", code)

        # inject a trap timeouts:
        re.sub( r'       #handle errors', "       #handle errors\n       except socket.timeout as e:\n          print (' > timeout requesting: {0}; {1}'.format(url, e))\n          return False,None\n", code)
        
        # move into a temp dir:
        rundir = tempfile.gettempdir()+"/dl"
        if os.path.isdir(rundir):
            shutil.rmtree(rundir)
        os.makedirs(rundir)
        os.chdir(rundir)

        #  Call the download summary and capture output
        print ("... Attempting downloads ...")

        with io.StringIO() as buf, redirect_stdout(buf):
            exec(code) 								#pylint: disable=exec-used
            out_text = buf.getvalue()
            shutil.rmtree(rundir)

        # Split by newline + carriage return
        lines = re.split("[\r\n]", out_text)
        # Find the failures:
        scrollback = []
        error_lines = []
        while True:
            if "Download Summary" in lines[0]:
                # We hit the summary
                break

            # Add it to scrollback
            next_line = lines.pop(0)
            scrollback.append(next_line)
            if len(scrollback) > 5:
                # Keep the last 5 lines
                scrollback.pop(0)
            # See if the last line has 'Error' in it
            if re.search('problem', scrollback[-1], re.IGNORECASE):
                for l in scrollback:
                    error_lines.append(l)
                scrollback = []

        # Find the the download report
        while True:
            if "Download Summary" in lines.pop(0):
                break
        if "Failure" in out_text:
            print(" >>>>> Encounted a problem!!!")
            print("out: {0}".format(out_text))
            send_sns(message="Encounted a problem!!!\n\n"+"\n".join(lines)+"\n\nError Log Output (Error + Context):\n ... "+"\n ... ".join(error_lines), subject="Failure!")
            return (False)
        else:
            print("All downloads seemed to be successful!")
            print("\n".join(lines))
            send_sns(message="All downloads seemed to be successful!\n\n"+"\n".join(lines), subject="Success!")
            return(True)
    except ValueError as E:
        print ("Problem fetching HTTP: {0}".format(E))
        send_sns(message="Problem fetching HTTP: {0}".format(E), subject="HTTP Error!")
    except Exception as E:
        print ("problem running code: {0}".format(E))
        send_sns(message="problem running code: {0}".format(E), subject="Error!")
        raise (E)

    # Remove run environment
    shutil.rmtree(rundir)
    return(False)
