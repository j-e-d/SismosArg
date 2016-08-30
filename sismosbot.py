#!/usr/bin/python3
# -*- coding: utf-8 -*-
import logging
import os
import sqlite3
import time

import lxml.etree as ET
import requests
import sys
import tweepy

if 'TESTING' in os.environ:
    if os.environ['TESTING'] == 'False':
        TESTING = False
    else:
        TESTING = True
else:
    TESTING = True

if 'LOG_FOLDER' in os.environ:
    LOG_FOLDER = os.environ['LOG_FOLDER']
else:
    LOG_FOLDER = ''

LIMIT = 4.0


def get_image(url, filename):
    r = requests.get(url=url, stream=True)
    if TESTING:
        print (r.url)
        print (r.status_code)
    if r.status_code == 200:
        with open(filename, 'wb') as f:
            for chunk in r:
                f.write(chunk)
        return True
    else:
        logging.warning('Getting image %s failed with %s', filename,
                        r.status_code)
        os.remove(filename)
        return False
    return False


def media_upload(api, filename):
    if TESTING:
        return 1
    try:
        response = api.media_upload(filename)
    except:
        print (sys.exc_info()[0])
        logging.exception('Upload media failed')
        return False
    return response.media_id_string


def scrape_last_events(c, url, api):
    r = requests.get(url)
    doc = ET.XML(r.content)
    for item in doc.xpath('//item'):
        title = item.xpath('./title')[0].text
        elements = title.split('--')
        order = elements[0].strip()
        date_event = elements[1].strip()
        time_event = elements[2].strip()
        lat = elements[3].strip()
        lon = elements[4].strip()
        magnitude = float(elements[5].strip())
        depth = elements[6].strip()
        zone = elements[7].strip()
        link = item.xpath('./link')[0].text
        sismo_id = link.split('/')[-1][:-1]
        description = item.xpath('./description')[0].text.split(
            '.')[0].split(',')[0]
        checked = item.xpath('./estado')[0].text

        image_filename = '{}.jpg'.format(sismo_id)
        image_url = 'http://www.inpres.gov.ar/desktop/mapas/{}'.format(
            image_filename)

        if TESTING:
            print(order, date_event, time_event, lat, lon, depth, magnitude,
                  zone, sismo_id, checked, description, image_url)

        # add to DB if not there
        c.execute('''SELECT COUNT() FROM sismos
                    WHERE identificador = %d''' % int(sismo_id))
        count = c.fetchone()[0]
        if count == 0:
            if not TESTING:
                c.execute('''INSERT INTO sismos
                          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                          (int(time.time()), date_event, time_event, lat, lon,
                           depth, magnitude, zone, sismo_id, checked,
                           description))
            if (int(time_event.split(':')[0])) == 1:
                plural = ''
            else:
                plural = 's'

            if magnitude >= LIMIT:
                get_image(image_url, image_filename)
                images = list()
                image = media_upload(api, image_filename)
                if image is not False:
                    images.append(image)

                text = ('Sismo de {0}˚, con epicentro {1} '
                        'registrado a la{2} {3} http://www.'
                        'inpres.gov.ar/desktop/epicentro1.php?s={4}'
                        ).format(magnitude,
                                 description,
                                 plural,
                                 time_event[:-4],
                                 sismo_id)
                logging.info('About to tweet %s, with media %s', text,
                             image_url)
                if TESTING:
                    url_length = 23
                else:
                    config = api.configuration()
                    url_length = int(config['short_url_length'])
                url_ori = len('http://www.inpres.gov.ar/desktop/') + \
                    len('epicentro1.php?s=') + len(sismo_id)
                text_len = len(text) - url_ori + url_length
                if text_len > 140:
                    text = text[:140]
                if TESTING:
                    print (text_len)
                    print (text, lat, lon, images)
                    os.remove(image_filename)
                else:
                    try:
                        api.update_status(text, lat=lat, long=lon,
                                          media_ids=images)
                    except:
                        print (sys.exc_info()[0])
                        logging.exception('Update status failed')
                    os.remove(image_filename)
    return


def create_db():
    conn = sqlite3.connect('sismosarg.db')
    c = conn.cursor()

    # Create table
    c.execute('''CREATE TABLE sismos
                 (datetime integer, fecha text, hora text, lat real,
                  lon real, profundidad integer, magnitud real,
                  zona text, identificador text, tipo text,
                  sentido integer)''')

    conn.commit()
    conn.close()


def get_last_id(c):
    c.execute('''SELECT identificador FROM sismos
                 ORDER BY datetime DESC LIMIT 1''')
    last_id = c.fetchone()
    if last_id is None:
        return last_id
    else:
        return last_id[0]


def test_twitter(api):
    print(api.me().name)


def main():
    # logging
    logging.basicConfig(
        filename=LOG_FOLDER + 'sismos.log',
        format='%(asctime)s %(name)s %(levelname)8s: %(message)s',
        level=logging.INFO)
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.info('Starting script')

    # keys
    consumer_key = os.environ['TWITTER_CONSUMER_KEY']
    consumer_secret = os.environ['TWITTER_CONSUMER_SECRET']
    access_token = os.environ['TWITTER_ACCESS_TOKEN']
    access_token_secret = os.environ['TWITTER_ACCESS_TOKEN_SECRET']

    if TESTING:
        api = None
    else:
        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.secure = True
        auth.set_access_token(access_token, access_token_secret)
        api = tweepy.API(auth)

    url = 'http://contenidos.inpres.gov.ar/rss/ultimos50.xml'

    # TODO: call create_db if db file not present
    conn = sqlite3.connect('sismosarg.db')
    c = conn.cursor()

    scrape_last_events(c, url, api)

    conn.commit()
    conn.close()
    logging.info('Finished script')


if __name__ == '__main__':
    main()
