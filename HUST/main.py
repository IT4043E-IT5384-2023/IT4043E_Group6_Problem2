from crawlQuestN import crawl
from concateUser import user_data
from classifyProject import classifyProject

def run():
    crawl()
    classifyProject()
    user_data()

if __name__ == "__main__":
    run()