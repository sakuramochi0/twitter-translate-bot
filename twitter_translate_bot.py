#!/usr/bin/env python3
import yaml
import re
import time
import json
import requests
from html import unescape
from argparse import ArgumentParser
from get_mongo_client import get_mongo_client
from get_tweepy import *
from googleapiclient.discovery import build

def save_tweet(screen_names):
    """Save new tweets of given screen_names to database."""
    for sn in screen_names:
        ts = api.user_timeline(screen_name=sn, count=200)
        for t in ts:
            if c.find({'_id': t.id}).count() == 0:
                doc = make_doc(t)
                if is_already_tweeted(t):
                    doc['meta']['tweeted'] = True
                c.insert(doc)

def is_already_tweeted(t):
    return hasattr(t, 'retweeted_status') and \
        c.find({
            'meta.retweeted': True,
            't.retweeted_status.id': t.retweeted_status.id,
        }).count() != 0
    
def make_doc(t):
    """Make mongoDB document from Tweepy status object."""
    doc = {
        '_id': t.id,
        'meta': {
            'tweeted': False,
            'google': {
                'translated': False,
                'post_processed': False,
            },
            'naver': {
                'translated': False,
                'post_processed': False,
            },
        },
        'time': t.created_at,
        'translated_text': {
            'google': {
                'raw': '',
                'post_processed': '',
            },
            'naver': {
                'raw': '',
                'post_processed': '',
            },
        },
        't': t._json,
    }
    return doc

def translate_untranslated_docs(google=None, naver=None):
    """Translate untranslated text in database."""
    for doc in c.find({'$or': [{'meta.google.translated': False}, {'meta.naver.translated': False}], 'meta.tweeted': False}):
        translated_texts = translate(doc['t'], google=google, naver=naver)
        if 'google' in translated_texts:
            google_translated = True
            google_text = translated_texts['google']
        else:
            google_translated = False
            google_text = ''
        if 'naver' in translated_texts:
            naver_translated = True
            naver_text = translated_texts['naver']
        else:
            naver_translated = False
            naver_text = ''
            
        c.update({'_id': doc['_id']},
                 {'$set': {
                     'translated_text.google.raw': google_text,
                     'translated_text.naver.raw': naver_text,
                     'meta.google.translated': google_translated,
                     'meta.naver.translated': naver_translated,
                     'meta.translated': True,
                 }})
        print(doc['t']['text'])
        print('->', translated_texts)
        print('-'*8)

def translate(t, google=None, naver=None):
    """Translate text of the tweet with Google and Naver translate API."""
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
            
    translated_texts = {'google': '', 'naver': ''}
    if google:
        translated_texts['google'] = translate_with_google(t['text'])
    if naver:
        translated_texts['naver'] = translate_with_naver(t['text'])

    if url_stack:
        for service, text in translated_texts.copy().items():
            if text:
                translated_texts[service] = text + ' ' + ' '.join(url_stack)

    return translated_texts

def pre_process(text, target_lang='ja'):
    """Pre-process text before Google Translate."""
    # Replace @ to avoid repling
    text = text.replace('@', '+')
    # Pre-replace before translate
    for ko, ja in correct_dict['pre'].items():
        if target_lang == 'ja':
            text = text.replace(ko, ja)
        else:
            text = text.replace(ja, ko)
    return text

def translate_with_google(text):
    """Translate text from Korean into Japanse by Google Translate API."""
    if not text:
        return ''
    res = service.translations().list(
        target=settings['target_lang'], q=text
    ).execute()
    if len(res['translations']) == 1:
        return res['translations'][0]['translatedText']
    else:
        return [obj['translatedText'] for obj in res['translations']]

def translate_with_naver(text):
    """Translate text from Korean to Japanese with Naver Translate API."""
    if not text:
        return ''
    # Prepare
    api_url = 'https://openapi.naver.com/v1/language/translate'
    creds = get_credencials()
    naver_id = creds[args.account]['naver-api-id']
    naver_secret = creds[args.account]['naver-api-secret']
    data = {
        'source': settings['source_lang'],
        'target': settings['target_lang'],
        'text': text,
    }
    headers = {
        'X-Naver-Client-Id': naver_id,
        'X-Naver-Client-Secret': naver_secret,
    }

    # Get result
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
        cursor = c.find({'meta.google.translated': True})
    else:
        cursor = c.find({
            'meta.google.translated': True,
            'meta.google.post_processed': False
        })
    for doc in cursor:
        google_text = post_process(doc['translated_text']['google']['raw'])
        naver_text = post_process(doc['translated_text']['naver']['raw'])
        if google_text:
            google_postprocessed = True
        else:
            google_postprocessed = False
        if naver_text:
            naver_postprocessed = True
        else:
            naver_postprocessed = False

        c.update({'_id': doc['_id']},
                 {'$set': {
                     'translated_text.google.post_processed': google_text,
                     'translated_text.naver.post_processed': naver_text,
                     'meta.google.post_processed': google_postprocessed,
                     'meta.naver.post_processed': naver_postprocessed,
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
    """Tweet untweeted tweets."""
    docs = c.find({
        'meta.google.translated': True,
        'meta.google.post_processed': True,
        'meta.tweeted': False,
    }).sort('_id')
    for doc in docs:
        tweet_doc(doc)
        time.sleep(5)

def tweet_doc(doc):
    """Tweet translated text in the doc."""
    id = doc['_id']
    tweet_url = make_tweet_url(doc['t'])
    texts = (doc['translated_text']['google']['post_processed'],
             doc['translated_text']['naver']['post_processed'])
    for service, texts in doc['translated_text'].items():
        text = texts['post_processed']
        if not text:
            continue
        if service == 'naver':
            text = 'N/' + text
        
        # If enough short tweet, just tweet it
        if len(text) < max_tweet_len - t_co_len:
            status = text + ' ' + tweet_url
            tweet(id, status, last=True)
        # Else long tweet, tweet first part
        else:
            max_body_len = max_tweet_len - t_co_len - 12
            status = text[:max_body_len] + ' ' + tweet_url
            print(len(status), status)
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
        res = api.update_status(
            status=status,
            in_reply_to_status_id=reply_id
        )
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
    """Load correct dictionary for bad translations."""
    with open('correct_dict.yaml') as f:
        correct_dict = yaml.load(f)
    return correct_dict

def get_credencials():
    """Get credencial data."""
    with open('credencials.yaml') as f:
        creds = yaml.load(f)
    return creds

def get_settings(account):
    """Get setting data."""
    with open('settings.yaml') as f:
        settings = yaml.load(f)
    return settings[account]

if __name__ == '__main__':
    # Parse arguments
    parser = ArgumentParser()
    parser.add_argument('account')
    parser.add_argument('commands', choices=[
        'save_tweet',
        'translate',
        'post_process',
        'tweet',
    ], nargs='+')
    parser.add_argument('--force', action='store_true')
    parser.add_argument('--google', action='store_true')
    parser.add_argument('--naver', action='store_true')
    args = parser.parse_args()

    # Prepare objects
    creds = get_credencials()
    settings = get_settings(args.account)
    service = build('translate', 'v2', developerKey=creds['google-api-key'])
    correct_dict = get_correct_dict()

    api = get_api(args.account)
    c = get_mongo_client()[settings['database_name']].tweets
    target_accounts = settings['target_accounts']

    # constants
    max_tweet_len = 100
    t_co_len = 24
    my_reply_screen_name = '@{sn} '.format(sn=args.account)

    # Run command
    for cmd in args.commands:
        if cmd == 'save_tweet':
            save_tweet(target_accounts)
        elif cmd == 'translate':
            translate_untranslated_docs(args.google, args.naver)
        elif cmd == 'post_process':
            do_post_process(force=args.force)
        elif cmd == 'tweet':
            do_tweet()
