import os
import json
import pandas as pd

save_path = "Twitter/data/twitter_username_count.csv"
users_by_quest_path = "QuestN/data/users"

def get_user_name(users_by_quest_path, save_path):
    df = pd.DataFrame(columns=["user_id",
                               "user_name",
                               "user_photo",
                               "user_background", "user_address", "discord_username", "twitter_username", "twitter_name"
                               ])

    for filename in os.listdir(users_by_quest_path):
        try:
            tmp_df = pd.read_csv(os.path.join(users_by_quest_path, filename))
            df = pd.concat([df, tmp_df])
        except:
            pass

    df.sort_values(by=["user_id"], inplace=True)
    value_counts = df[['twitter_username']
                      ].value_counts().sort_values(ascending=False)
    value_counts = value_counts.reset_index()
    value_counts.to_csv(save_path)

if __name__ == "__main__":
    get_user_name(users_by_quest_path, save_path)
