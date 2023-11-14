import os
import time
import json
import pandas as pd
from datetime import datetime
import requests

root_path = 'Twitter/data'
headers = {
    "user-agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1",
    'cookie': '_ga=GA1.2.1296315602.1699583836; _gid=GA1.2.1873221091.1699583836; guest_id=v1%3A169958383610341599; _twitter_sess=BAh7CSIKZmxhc2hJQzonQWN0aW9uQ29udHJvbGxlcjo6Rmxhc2g6OkZsYXNo%250ASGFzaHsABjoKQHVzZWR7ADoPY3JlYXRlZF9hdGwrCJ9GF7eLAToMY3NyZl9p%250AZCIlYzczM2I3ZjExMmEyNzA4NWU5YTk1YTM5Y2E2OTBhNTQ6B2lkIiU0ZmM1%250AMjI0ZDM4ODg0YWMwOWU4OGVhY2ViNGFhMDk1Zg%253D%253D--b2991a2a5136ee8e20af2ea485ca487250bee35c; kdt=DTeUKiXRthMvs79A7uFam4oGGE3WghsoNGbBrspi; auth_token=d1028cba0fdfce71a04f5147eb874fec9584561e; ct0=6f7a03fd96e7b767ad64111829677c619e5b107078a6a76150e6ad4f453e533b954bcc5e094ed5182a4eaac8d7c58c6d16c1de22aa5770484e7c3d333184b90e0b43671512da32c18591202c8261984e; guest_id_ads=v1%3A169958383610341599; guest_id_marketing=v1%3A169958383610341599; lang=en; twid=u%3D1381520495816810504; att=1-SRLUDYFQ9hfWdyXQqoxNm1SSaD8sdhZD3NHw6yKz; personalization_id="v1_9XCFs5uyRy43M7hCME/73Q=="',
    'x-client-transaction-id': 'jl7qPRY0lfssGijq98LsgUe9W5HiF1UpDwYHEn3s38fymxA/53cYZxetzVZ3h/fHubu6cI72RThZHe575eU4PpUo/auzjw',
    'x-csrf-token': '6f7a03fd96e7b767ad64111829677c619e5b107078a6a76150e6ad4f453e533b954bcc5e094ed5182a4eaac8d7c58c6d16c1de22aa5770484e7c3d333184b90e0b43671512da32c18591202c8261984e',
    'x-twitter-active-user': 'yes',
    'x-twitter-auth-type': 'OAuth2Session',
    'x-twitter-client-language': 'en',
    'authorization': 'Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA'
}

def get_user_id(username: str):
  global headers
  try:
    resp = requests.get(
        f"https://twitter.com/i/api/graphql/_pnlqeTOtnpbIL9o-fS_pg/ProfileSpotlightsQuery?variables=%7B%22screen_name%22%3A%22{username}%22%7D", headers=headers)
  except Exception as e:
    print(e)
    print(resp.text)
    print(resp.status_code)

  if 'user_result_by_screen_name' not in resp.json()['data'].keys():
    return None
  
  if resp.json()['data']['user_result_by_screen_name']['result']['__typename'] == 'UserUnavailable':
    return None
  
  return resp.json()['data']['user_result_by_screen_name']['result']['rest_id']


def get_base_url(user_id: str, cursor_str: str=''):
  if len(cursor_str) == 0:
    with_cursor = ''
  else:
    with_cursor = f'%22cursor%22%3A%22{cursor_str}%22%2C'
  return f'https://twitter.com/i/api/graphql/VgitpdpNZ-RUIp5D1Z_D-A/UserTweets?variables=%7B%22userId%22%3A%22{user_id}%22%2C%22count%22%3A20%2C{with_cursor}%22includePromotedContent%22%3Atrue%2C%22withQuickPromoteEligibilityTweetFields%22%3Atrue%2C%22withVoice%22%3Atrue%2C%22withV2Timeline%22%3Atrue%7D&features=%7B%22responsive_web_graphql_exclude_directive_enabled%22%3Atrue%2C%22verified_phone_label_enabled%22%3Afalse%2C%22responsive_web_home_pinned_timelines_enabled%22%3Atrue%2C%22creator_subscriptions_tweet_preview_api_enabled%22%3Atrue%2C%22responsive_web_graphql_timeline_navigation_enabled%22%3Atrue%2C%22responsive_web_graphql_skip_user_profile_image_extensions_enabled%22%3Afalse%2C%22c9s_tweet_anatomy_moderator_badge_enabled%22%3Atrue%2C%22tweetypie_unmention_optimization_enabled%22%3Atrue%2C%22responsive_web_edit_tweet_api_enabled%22%3Atrue%2C%22graphql_is_translatable_rweb_tweet_is_translatable_enabled%22%3Atrue%2C%22view_counts_everywhere_api_enabled%22%3Atrue%2C%22longform_notetweets_consumption_enabled%22%3Atrue%2C%22responsive_web_twitter_article_tweet_consumption_enabled%22%3Afalse%2C%22tweet_awards_web_tipping_enabled%22%3Afalse%2C%22freedom_of_speech_not_reach_fetch_enabled%22%3Atrue%2C%22standardized_nudges_misinfo%22%3Atrue%2C%22tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled%22%3Atrue%2C%22longform_notetweets_rich_text_read_enabled%22%3Atrue%2C%22longform_notetweets_inline_media_enabled%22%3Atrue%2C%22responsive_web_media_download_video_enabled%22%3Afalse%2C%22responsive_web_enhance_cards_enabled%22%3Afalse%7D'


def crawl_data(usernames: list, crawled_users: list):
  global root_path
  BREAK_ALL = False

  for username in usernames:
    if username in crawled_users:
      continue
    
    print(f"Crawl tweets of user {username}")
    user_id = get_user_id(username)
    if user_id is None:
      continue

    full_data = []
    c = 0

    r = requests.get(get_base_url(user_id=user_id), headers=headers)

    data_len = 0
    if 'result' in r.json()['data']['user'].keys():
      data_len = len(r.json()['data']['user']['result']
                    ['timeline_v2']['timeline']['instructions'][-1]['entries'])

    while data_len >= 20:
      all_entries = r.json()[
          'data']['user']['result']['timeline_v2']['timeline']['instructions'][-1]['entries']
      data_len = len(all_entries)
      full_data += all_entries
      cursor_str = all_entries[-1]['content']['value']
      try:
        r = requests.get(get_base_url(user_id=user_id, cursor_str=cursor_str), headers=headers)
      except Exception as e:
        print(e)
        BREAK_ALL = True
        break

      if 'itemContent' in full_data[-3]['content'].keys():
        result_full_data = full_data[-3]['content']['itemContent']['tweet_results']
      elif 'items' in full_data[-3]['content'].keys():
        result_full_data = full_data[-3]['content']['items'][0]['item']['itemContent']['tweet_results']
      else:
        break

      if 'result' in result_full_data.keys():
        if 'legacy' in result_full_data['result'].keys():
          datetime_str = result_full_data['result']['legacy']['created_at']
        else:
          datetime_str = result_full_data['result']['tweet']['legacy']['created_at']
      else:
        datetime_str = result_full_data['tweet']['legacy']['created_at']
      datetime_true_format = datetime.strptime(
          datetime_str, '%a %b %d %H:%M:%S +0000 %Y')
      time.sleep(30)
      c += 1
      if datetime_true_format < datetime(2023, 10, 15):
        break

    if BREAK_ALL:
      break
    else:
      with open(f'{root_path}/Tweets - after 15 10 2023/{username}.json', 'w') as f:
        json.dump(full_data, f)


if __name__ == "__main__":
  if not os.path.exists(root_path):
    os.makedirs(root_path)

  tweets_path = f'{root_path}/Tweets - after 15 10 2023'
  if not os.path.exists(tweets_path):
    os.makedirs(tweets_path)

  usernames = pd.read_csv(f'{root_path}/twitter_username_count.csv')['twitter_username'].tolist()
  crawled_users = [i.replace('.json', '') for i in os.listdir(tweets_path)]

  crawl_data(usernames, crawled_users)
