import configparser, sys
import logging
import vk
import json
import time
import random 
import pandas as pd
import argparse
import re
from flatten_json import flatten
from sklearn.feature_extraction.text import TfidfVectorizer


USERS_JSON = 'users.json'
POSTS_JSON = 'posts.json'
POSTS_CSV = 'posts.csv'

def load_json(filepath):
    with open(filepath, "r", encoding='utf-8') as input_file:
        raw_json_data = json.load(input_file)
    return raw_json_data

def scrape_data(api):
    ucount=0
    post_list = []
    user_list = []
    post_slice = 100
    post_limit = 300
    random.seed(int(time.time()))
    user_dict = {}
    while len(user_dict.keys()) < 100:
        user_id=random.randint(1, 219000000)
        if user_id in user_dict:
            print('user in dict areasy')
            continue
        try:
            user_info = api.users.get(user_ids=user_id,
                                      fields='sex,bdate,country,home_town,contacts,about,books,activities,\
                                             interests,personal,relation,music,movies,education,universities')[0]
            user_info['byear'] = None
            if not 'bdate' in user_info:
                continue
            m = re.search(r'(\d{4})', user_info['bdate'])
            if m:
                user_info['byear'] = m.group(1)
                print("year = %s" % user_info['byear'])
                posts = api.wall.get(owner_id=user_id, filter='owner', count=1)
                if posts['count'] > post_slice:
                    print("post count = %s" % posts['count'])
                    stop = post['count'] if posts['count'] < post_limit else post_limit
                    for offset in range(0, stop, post_slice):
                        time.sleep(1)
                        posts = api.wall.get(owner_id=user_id, offset=offset, filter='owner', count=post_slice)
                        for post in posts['items']:
                            post_list.append(post);

                    user_dict[user_info['id']] = user_info
                    print(['user len', len(user_dict.keys())])
                    #print(json.dumps(user_info, sort_keys=True, indent=4, ensure_ascii=False))
        except Exception as e:
            print(str(e))
            time.sleep(1)

    print(len(post_list))
    print(len(user_dict.keys()))
    with open(USERS_JSON, 'w', encoding='utf-8') as f_users:
        f_users.write(json.dumps(list(user_dict.values()), sort_keys=True, ensure_ascii=False))
    with open(POSTS_JSON, 'w', encoding='utf-8') as f_posts:
        f_posts.write(json.dumps(post_list, sort_keys=True, ensure_ascii=False))


def read_users():
    dic_flattened = (flatten(d) for d in load_json(USERS_JSON))
    return pd.DataFrame(dic_flattened)


def read_posts():
    """
    keys for post:
    * text
    * comments_count
    * likes_count
    * reposts_count
    * date
    * post_type
    * views_count
    * copy_history_text
    * copy_history_post_type
    * attachments_video_count
    * attachments_photo_count
    * attachments_audio_count
    """
    jposts = load_json(POSTS_JSON)
    post_list = []
    for post in jposts:
        for key, value  in post.items():
            ch = [None, None]
            if 'copy_history' in post:
                ch = [post['copy_history'][0]['text'],
                      post['copy_history'][0]['post_type']]
            att = [None, None, None]
            if 'attachments' in post:
                att = [
                        len(list(
                            filter(lambda x: x['type'] == 'video',
                                   post['attachments']))),
                        len(list(
                            filter(lambda x: x['type'] == 'photo',
                                   post['attachments']))),
                        len(list(
                            filter(lambda x: x['type'] == 'audio',
                                   post['attachments'])))
                    ]
            post_list.append([post['owner_id'],
                              post['text'],
                              post['comments']['count'],
                              post['likes']['count'],
                              post['reposts']['count'],
                              post['date'],
                              post['post_type'],
                              post['views']['count'] if 'views' in post else 0
                              ] + ch + att)

    post_df = pd.DataFrame(post_list, columns=['owner_id', 'text', 'comments_count', 'likes_count',
                                    'reposts_count', 'date',
                                    'post_type', 'views_count', 'copy_history_text',
                                    'copy_history_post_type',
                                    'attachments_video_count',
                                    'attachments_photo_count',
                                    'attachments_audio_count'
                                    ])
    #post_df.to_csv(POSTS_CSV)
    return post_df

def tdidf_transform(text_list):
    vectorizer = TfidfVectorizer()
    features = vectorizer.fit_transform(text_list).todense()
    print(len(features))


def read_transform():
    user_df = read_users()
    post_df = read_posts()

    print(user_df.columns)
    print(user_df.head(5))

    #post_df = post_df.dropna(axis=1, thresh=1000)
    print(post_df.columns)
    tdidf_transform(post_df['text'].sample(100).tolist())
    #print(post_df.sample(5))
    #print(post_df.describe())

    

def get_args():
    parser = argparse.ArgumentParser(description='vk parser and analizer.')
    parser.add_argument('-a', '--action',
                        help='choose an action - scrape/read',
                        required=True)
    args = parser.parse_args()
    return args


def vk_auth(config):
    config.read('vkstat.cfg')
    session = vk.AuthSession(app_id=config['vkapi']['client_id'],
                             access_token=config['vkapi']['token'])
    return vk.API(session, v=config['vkapi']['v'])

    
if __name__ == '__main__':
    args = get_args()
    config = configparser.ConfigParser()
    api = vk_auth(config=config)
    if args.action == 'scrape':
        scrape_data(api)
    elif args.action == 'read':
        read_transform()
    
    """
    posts = api.wall.get(filter='owner', count=10)
    for post in posts['items']:
        print(json.dumps(post, sort_keys=True, indent=4, ensure_ascii=False))
    """
