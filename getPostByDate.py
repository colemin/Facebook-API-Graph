import json
import datetime
import csv
import time

try:
    from urllib.request import urlopen, Request
except ImportError:
    from urllib2 import urlopen, Request

page_id = ''
since_date = ''
until_date = ''
access_token = "EAACEdEose0cBACg0UeVJGNZCNxAao4Ua2Vkd5MZAoy82bAQ8iTtx4R338j3DiOuLInixI6qHwfGfVoH1kmfxgXI0KvawMYpXN7WDnpjty9Qb07OdLx0QZAHIPs7Q9FLZAuJZCPd6IjIjPKyYsQ2rUnFu9Ud8lvp02ZClZCfzMzL2oAlMALh0339jZAEmZA2Q0eyMZD"
file_path = '/Users/jesus/jetBrains/PycharmProjects/test/Sentiment/src'
limit = 100


# Creamos la URL para la peticion usando Graph API Explorer
def buildURL(version, query, since, until):
    base = "https://graph.facebook.com/v{}/".format(version)
    query = query.replace('{','%7B').replace('}','%7D')
    parameters = "&limit={}&access_token={}".format(limit, access_token)
    since = "&since={}".format(since_date) if since is not '' else ''
    until = "&until={}".format(until_date) if until is not '' else ''
    url = base + query + parameters + since + until
    return(url)

# Para evitar caidas por fallos de conexion, me espero 5 segundos y lo vuelvo a intentar.
def request_until_succeed(url):
    req = Request(url)
    success = False
    while success is False:
        try:
            response = urlopen(req)
            if response.getcode() == 200:
                success = True
        except Exception as e:
            print(e)
            time.sleep(5)

            print("Error para la URL {}: {}".format(url,datetime.datetime.now()))
            print("Volvemos a intentarlo...")

    return response.read()

# Para leer bien todos los caracteres del mensaje
def unicode_decode(text):
    try:
        return text.encode('utf-8').decode().replace('\n', '. ').replace('\r', '')
    except UnicodeDecodeError:
        return text.encode('utf-8').replace('\n', '').replace('\r', '')

# Facebook nos da la hora en UTC del ESTE, la convertimos a la nuestra (+1)
def time_decode(created_time_in):
    created_time_out = datetime.datetime.strptime(
        created_time_in, '%Y-%m-%dT%H:%M:%S+0000')
    created_time_out = created_time_out + \
        datetime.timedelta(hours=+1)
    created_time_out = created_time_out.strftime(
        '%Y-%m-%d %H:%M:%S')
    return created_time_out

def scrapeFacebookPosts(postFont):
    # Creo fichero para escribir los "posts" o los "visitor_posts" dependiendo de la fuente.
    with open ('{}/{}_facebook_posts.csv'.format(file_path, page_id), 'w') as file:
        w = csv.writer (file)
        w.writerow (["font","id", "from_id", "from_name", "type", "created_time", "link", "message",
                     "num_shares",
                     "num_comments",
                     "num_likes", "num_loves", "num_wows", "num_hahas","num_sads", "num_angrys"])

        has_next_page = True
        #query = "{}/{}?fields=from,message,id,created_time,link,type,shares,comments.limit(0).summary(1),reactions.type(LIKE).limit(0).summary(1).as(like),reactions.type(LOVE).limit(0).summary(1).as(love),reactions.type(HAHA).limit(0).summary(1).as(haha),reactions.type(WOW).limit(0).summary(1).as(wow),reactions.type(SAD).limit(0).summary(1).as(sad),reactions.type(ANGRY).limit(0).summary(1).as(angry)".format(page_id, postFont)

        query = "{}/{}?fields=from,message,id,created_time,permalink_url,shares,type," \
                "comments.limit(0).summary(1)," \
                "reactions.type(LIKE).limit(0).summary(1).as(like)," \
                "reactions.type(LOVE).limit(0).summary(1).as(love)," \
                "reactions.type(HAHA).limit(0).summary(1).as(haha)," \
                "reactions.type(WOW).limit(0).summary(1).as(wow)," \
                "reactions.type(SAD).limit(0).summary(1).as(sad)," \
                "reactions.type(ANGRY).limit(0).summary(1).as(angry)".format(page_id, postFont)

        url = buildURL ('2.10', query, since_date, until_date)
        #print(url)
        loop = 0
        while has_next_page:
            num_processed = 0
            loop += 1
            posts = json.loads(request_until_succeed(url))
            for post in posts['data']:
                num_processed += 1

                message = unicode_decode(post['message']) if post.has_key('message') else ''
                link = unicode_decode(post['permalink_url']) if post.has_key('permalink_url') else ''
                shares = post['shares']['count'] if post.has_key ('shares') else 0

                w.writerow([postFont, post['id'], post['from']['id'], unicode_decode(post['from']['name']),post['type'],time_decode(post['created_time']), link, message, shares, post['comments']['summary']['total_count'],post['like']['summary']['total_count'],post['love']['summary']['total_count'],post['haha']['summary']['total_count'],post['wow']['summary']['total_count'],post['sad']['summary']['total_count'],post['angry']['summary']['total_count']])

            if 'next' in posts['paging']:
                url = posts['paging']['next']
                print("{} - {} Bloque de {}  \"{}\" \t Procesados = {}".format(datetime.datetime.now(),loop, limit, postFont.upper(),limit*loop))
            else:
                has_next_page = False
                print("{} - {} Bloque de {}  \"{}\" \t Procesados = {}".format(datetime.datetime.now(),loop, num_processed, postFont.upper(), limit*(loop-1)+num_processed))


if __name__ == "__main__":

    page_id = raw_input ("\nDe que pagina Facebook quieres los posts:[vueling] ") or 'vueling'
    since_date = raw_input ("DESDE que fecha quieres los posts (YYYY-MM-DD): [2017-12-01] ") or '2017-12-01'
    until = (datetime.datetime.now () + datetime.timedelta (days=1)).strftime("%Y-%m-%d")
    until_date = raw_input ("HASTA que fecha quieres los posts (YYYY-MM-DD): [{}] ".format(until)) or until
    print('\n---------------------')
    scrapeFacebookPosts('feed')


