import configparser, sys
import logging
import vk
import json
import time
import random 
import pandas as pd

USERS_JSON = 'users.json'
POSTS_JSON = 'posts.json'

def scrape_data(api):
    ucount=0
    post_list = []
    user_list = []
    post_slice = 100
    post_limit = 300
    random.seed(int(time.time()))
    while ucount < 100:
        user_id=random.randint(1, 219000000)
        try:
            posts = api.wall.get(owner_id=user_id, filter='owner', count=1)
            if posts['count'] > post_slice:
                ucount += 1
                print(posts['count'])
                stop = post['count'] if posts['count'] < post_limit else post_limit
                for offset in range(0, stop, post_slice):
                    time.sleep(1)
                    posts = api.wall.get(owner_id=user_id, offset=offset, filter='owner', count=post_slice)
                    for post in posts['items']:
                        post_list.append(post);
                        
                user_info = api.users.get(user_ids=user_id,
                                          fields='sex,bdate,country,home_town,contacts,about,books,activities,\
                                                 interests,personal,relation,music,movies,education,universities')
                user_list.append(user_info)
                print(['user len', len(user_list)])
                #print(json.dumps(user_info, sort_keys=True, indent=4, ensure_ascii=False))
        except Exception as e:
            print(str(e))
            time.sleep(1)
    print(len(post_list))
    print(len(user_list))
    with open(USERS_JSON, 'w', encoding='utf-8') as f_users:
        f_users.write(json.dumps(user_list, sort_keys=True, ensure_ascii=False))
    with open(POSTS_JSON, 'w', encoding='utf-8') as f_posts:
        f_posts.write(json.dumps(post_list, sort_keys=True, ensure_ascii=False))

def read_users():
    df =  pd.read_json(USERS_JSON)
    

def transform():
    pass
    

def get_args():
    parser = argparse.ArgumentParser(description='vk parser and analizer.')
    parser.add_argument('-a', '--action',
                        help='choose an action - scrape/read',
                        required=True)
    args = parser.parse_args()
    return args


def vk_auth(config)
    config.read('vkstat.cfg')
    session = vk.AuthSession(app_id=config['vkapi']['client_id'],
                             access_token=config['vkapi']['token'])
    api = vk.API(session, v=config['vkapi']['v'])

    
if __name__ == '__main__':
    args = get_args()
    config = configparser.ConfigParser()
    vk_auth(config=config)
    if args.action == 'scrape':
        scrape_data(api)
    elif args.action == 'read':
    
    """
    posts = api.wall.get(filter='owner', count=10)
    for post in posts['items']:
        print(json.dumps(post, sort_keys=True, indent=4, ensure_ascii=False))
    """
