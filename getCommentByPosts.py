import json
import datetime
import csv
import time

try:
    from urllib.request import urlopen, Request
except ImportError:
    from urllib2 import urlopen, Request

access_token = "EAACEdEose0cBAAOik0bcEQCBUGPi4K6qd2ZBtxAXs2m7tQZCaeqHZCZCtwyRNxHVHShTK1uSwwDQX0j8avqLyibunCkJKQYUO35bc9DQu1x8VCNC6ZC1Lje5r7I7M5IvDjsLuiUrP86bd5EmVooeivZAPgPoyMgwfvcmhDWObmQWX4ZCd1moLFvE0QtYiATZC9gZD"
file_path = '/Users/jesus/jetBrains/PycharmProjects/test/Sentiment/src'
page_id = 'vueling'
limit = 100


# Creamos la URL para la peticion usando Graph API Explorer
def buildURL(version, query, since, until):
    base = "https://graph.facebook.com/v{}/".format (version)
    query = query.replace ('{', '%7B').replace ('}', '%7D')
    parameters = "&limit=({})&access_token={}".format (limit, access_token)
    since = "&since={}".format (since_date) if since is not '' else ''
    until = "&until={}".format (until_date) if until is not '' else ''
    url = base + query + parameters + since + until
    return (url)


# Para evitar caidas por fallos de conexion, me espero 5 segundos y lo vuelvo a intentar.
def request_until_succeed(url):
    req = Request (url)
    success = False
    while success is False:
        try:
            response = urlopen (req)
            if response.getcode () == 200:
                success = True
        except Exception as e:
            print(e)
            time.sleep (5)

            print("Error para la URL {}: {}".format (url, datetime.datetime.now ()))
            print("Volvemos a intentarlo...")

    return response.read ()


# Para leer bien todos los caracteres del mensaje
def unicode_decode(text):
    try:
        return text.encode ('utf-8').decode ().replace ('\n', '. ').replace ('\r', '')
    except UnicodeDecodeError:
        return text.encode ('utf-8').replace ('\n', '. ').replace ('\r', '')


# Facebook nos da la hora en UTC del ESTE, la convertimos a la nuestra
def time_decode(created_time_in):
    created_time_out = datetime.datetime.strptime (
        created_time_in, '%Y-%m-%dT%H:%M:%S+0000')
    created_time_out = created_time_out + \
                       datetime.timedelta (hours=+1)  # EST
    created_time_out = created_time_out.strftime (
        '%Y-%m-%d %H:%M:%S')  # best time format for spreadsheet programs
    return created_time_out


def scrapeFacebookResponses(responses, fileResponses):

    num_responses_processed = 0

    for i in range (len (responses['comments']['data'])):
        response = responses['comments']['data'][i]
        if i == 0:
            next = responses['id']
        else:
            next = responses['comments']['data'][i - 1]['id']
        num_responses_processed += 1

        link = unicode_decode (response['permalink_url']) if response.has_key ('permalink_url') else ''

        fileResponses.writerow (
            [responses['id'], next, response['id'], time_decode (response['created_time']), link,
             response['from']['id'],unicode_decode (response['from']['name']), unicode_decode (response['message'])])

    print("    {} - {} Responses Procesadas".format (datetime.datetime.now (),num_responses_processed))


def scrapeFacebookComments():
    # Creo fichero para escribir los Posts.

    with open ('{}/{}_facebook_comments.csv'.format (file_path, page_id), 'w') as file:
        fileComments = csv.writer (file)
        fileComments.writerow (
            ["parent_id", "id", "created_time", "link","from_id", "from_name", "message", "lang", "sentiment", "keywords"])

        with open ('{}/{}_facebook_responses.csv'.format (file_path, page_id), 'w') as file:
            fileResponses = csv.writer (file)
            fileResponses.writerow (
                ["parent_id", "next_id", "id", "created_time", "link", "from_id", "from_name", "message", "lang", "sentiment",
                 "keywords"])

            with open ('{}/input.csv'.format (file_path, page_id), 'rt') as csvpostfiles:
                reader = csv.DictReader (csvpostfiles)

                for post in reader:
                    has_next_page = True
                    query = post['id'] + '?fields=comments.limit({})'.format(limit) + '{id,created_time,permalink_url,message,from,comments{message,created_time,permalink_url,from}}'
                    urlcomments = buildURL ('2.10', query, '', '')

                    print("POST: {}\n-----------------------------------".format (post['id'], datetime.datetime.now ()))
                    num_total_comments_processed = 0
                    while has_next_page:
                        num_comments_processed = 0
                        comments = json.loads (request_until_succeed (urlcomments))

                        if 'comments' in comments:
                            listComments = comments['comments']
                        elif 'data' in comments:
                            listComments = comments
                        else:
                            listComments = {}

                        if len(listComments)>1:
                            for comment in listComments['data']:
                                num_comments_processed += 1
                                num_total_comments_processed += 1
                                link = unicode_decode (comment['permalink_url']) if comment.has_key ('permalink_url') else ''
                                fileComments.writerow ([post['id'], comment['id'], time_decode (comment['created_time']),
                                                        link, comment['from']['id'], unicode_decode (comment['from']['name']),
                                                        unicode_decode (comment['message'])])

                                print("  {} - {} Comments Procesados de {}".format (datetime.datetime.now (),
                                                                                    num_total_comments_processed,
                                                                                    post['num_comments']))

                                if 'comments' in comment:
                                    scrapeFacebookResponses (comment, fileResponses)

                            if 'next' in listComments['paging']:
                                urlcomments = listComments['paging']['next']
                            else:
                                has_next_page = False
                        else:
                            has_next_page = False

if __name__ == "__main__":
    scrapeFacebookComments ()

