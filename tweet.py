#!/usr/bin/env python3
import yaml
import re
import time
import json
import requests
from html import unescape
from argparse import ArgumentParser
from pymongo.mongo_client import MongoClient
from get_tweepy import *
from googleapiclient.discovery import build

def save_tweet():
    """
    Save new tweets of given screen_names to database.
    """
    for sn in screen_names:
        ts = api.user_timeline(screen_name=sn, count=200)
        for t in ts:
            if c.find({'_id': t.id}).count() == 0:
                doc = make_doc(t)
                c.insert(doc)
    
def make_doc(t):
    doc = {
        '_id': t.id,
        't': t._json,
        'time': t.created_at,
        'meta': {
            'translated': False,
            'post_processed': False,
            'tweeted': False,
        }
    }
    return doc

def translate_untranslated_docs():
    """
    Translate untranslated text in database.
    """
    for doc in c.find({'meta.translated': False}):
        translated_texts = translate(doc['t'])
        c.update({'_id': doc['_id']},
                 {'$set': {
                     'translated_text_raw': translated_texts[0],
                     'translated_text_naver_raw': translated_texts[1],
                     'meta.translated': True,
                 }})
        print(doc['t']['text'])
        print('->', translated_texts)
        print('-'*8)

def translate(t):
    """Translate text of the tweet with Google and Naver translate API.

    Args:
        t: tweet object :: tweepy.Status
    Return:
        translated_texts :: Tuple(str)
    """
    t['text'] = pre_process(t['text'])

    # Put aside or remove urls to reduce Google Translate cost.
    url_stack = []
    for url in t['entities']['urls']:
        url = url['url']
        url_stack.append(url)
        t['text'] = re.sub(r'\s*{url}\s*'.format(url=url), '', t['text'])
    if 'media' in t['entities']:
        for medium in t['entities']['media']:
            url = medium['url']
            t['text'] = re.sub(r'\s*{url}\s*'.format(url=url), '', t['text'])
            
    translated_texts = [translate_with_google(t['text']), translate_with_naver(t['text'])]

    if url_stack:
        for i in range(len(translated_texts)):
            translated_texts[i] += ' ' + ' '.join(url_stack)

    return translated_texts

def pre_process(text):
    """
    Pre-process text before Google Translate.
    """
    # Replace @ to avoid repling
    text = text.replace('@', '+')
    # Pre-replace before translate
    for ko, ja in correct_dict['pre'].items():
        text = text.replace(ko, ja)
    return text

def translate_with_google(text):
    """Translate text from Korean into Japanse by Google Translate API."""
    res = service.translations().list(target='ja', q=text).execute()
    if len(res['translations']) == 1:
        return res['translations'][0]['translatedText']
    else:
        return [obj['translatedText'] for obj in res['translations']]

def translate_with_naver(text):
    """Translate text from Korean to Japanese with Naver Translate API."""
    api_url = 'https://openapi.naver.com/v1/language/translate'
    creds = get_credencials()
    naver_id = creds['naver-api-id']
    naver_secret = creds['naver-api-secret']
    data = {
        'source': 'ko',
        'target': 'ja',
        'text': text,
    }
    headers = {
        'X-Naver-Client-Id': naver_id,
        'X-Naver-Client-Secret': naver_secret,
    }
    r = requests.post(api_url, data=data, headers=headers)
    res = json.loads(r.text)
    if 'translatedText' in res['message']['result']:
        return res['message']['result']['translatedText']
    elif 'translatedTexts' in res['message']['result']:
        return res['message']['result']['translatedTexts']
    else:
        raise ValueError('Return no valid result.', res)

def do_post_process(force=False):
    """
    Refine translations.
    """
    if force:
        cursor = c.find({'meta.translated': True})
    else:
        cursor = c.find({'meta.translated': True, 'meta.post_processed': False})
    for doc in cursor:
        translated_text = post_process(doc['translated_text_raw'])
        translated_naver_text = post_process(doc['translated_text_naver_raw'])
        c.update({'_id': doc['_id']},
                 {'$set': {
                     'translated_text': translated_text,
                     'translated_naver_text': translated_naver_text,
                     'meta.post_processed': True,
                 }})

def post_process(text):
    """
    Post-process text after Google Translate.
    """
    # Correct mistranslations
    for error, right in correct_dict['post'].items():
        text = text.replace(error, right)
    # Unescape html quote
    text = unescape(text)
    # Replace zenkaku to hankaku
    text = text.replace('＃', '#')
    # Remove unnecessary spaces
    m = re.search(r'(.*\+)\s*(\w{1,15}.+$)', text)
    if m:
        text = m.group(1) + m.group(2)
    return text

def do_tweet():
    """
    Tweet untweeted tweets.
    """
    docs = c.find({'meta.translated': True, 'meta.post_processed': True, 'meta.tweeted': False}).sort('_id')
    for doc in docs:
        tweet_doc(doc)
        time.sleep(5)

def tweet_doc(doc):
    """Tweet translated text in the doc."""
    id = doc['_id']
    tweet_url = make_tweet_url(doc['t'])
    for text in (doc['translated_text'], 'N/' + doc['translated_naver_text']):
        # If enough short tweet, just tweet it
        if len(text) < max_tweet_len - t_co_len:
            status = text + ' ' + tweet_url
            tweet(id, status, last=True)
        # Else long tweet, tweet first part
        else:
            max_body_len = max_tweet_len - t_co_len - 12
            status = text[:max_body_len] + ' ' + tweet_url
            print(len(status))
            reply_id = tweet(id, status, last=False)
            text = text[max_body_len:]
            # Then tweet remaining parts
            max_body_len = max_tweet_len - len(my_reply_screen_name)
            while True:
                if len(text) < max_body_len:
                    status = my_reply_screen_name + text
                    tweet(id, status, last=True, reply_id=reply_id)
                    break
                else:
                    status = my_reply_screen_name + text[:max_body_len-1]
                    reply_id = tweet(id, status, last=False, reply_id=id)
                    text = text[max_body_len-1:]

def tweet(id, status, last, reply_id=None):
    """
    Tweet to Twitter and record doc as tweeted.
    """
    try:
        res = api.update_status(status=status, in_reply_to_status_id=reply_id)
        if last:
            c.update({'_id': id}, {'$set': {'meta.tweeted': True}})
        return res.id
    except tweepy.TweepError as e:
        print(e)
        raise

def make_tweet_url(t):
    """Make tweet url string from tweet json."""
    return 'https://twitter.com/{sn}/status/{id}'.format(sn=t['user']['screen_name'], id=t['id'])

def get_correct_dict():
    """Load correct dictionary for Google's bad translations."""
    with open('correct_dict.yaml') as f:
        correct_dict = yaml.load(f)
    return correct_dict

def get_credencials():
    with open('credencials.yaml') as f:
        creds = yaml.load(f)
    return creds

if __name__ == '__main__':
    # Prepare objects
    creds = get_credencials()
    service = build('translate', 'v2', developerKey=creds['google-api-key'])
    api = get_api('rhythpri_ko_ja')
    c = MongoClient().rhythpri_ko_ja.tweets
    correct_dict = get_correct_dict()

    # Prepare data
    screen_names = ['anidong3282', 'PRIPARA_TV']
    max_tweet_len = 140
    t_co_len = 24
    my_reply_screen_name = '@' + api.auth.username + ' '

    # Parse arguments
    parser = ArgumentParser()
    parser.add_argument('--force', action='store_true')
    parser.add_argument('command', choices=[
        'save_tweet',
        'translate',
        'post_process',
        'tweet',
    ], nargs='+')
    args = parser.parse_args()

    # Run command
    for cmd in args.command:
        if cmd == 'save_tweet':
            save_tweet()
        if cmd == 'translate':
            translate_untranslated_docs()
        if cmd == 'post_process':
            if args.force:
                do_post_process(force=True)
            else:
                do_post_process()
        if cmd == 'tweet':
            do_tweet()
