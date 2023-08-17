import time

from RecommendationSystem import get_recommendations
from RedisSystem import get_recommendations_redis
from MongoDBSystem import get_recommendations_mongo

# inserting all the records

def inserting_time(rs):
    tic = time.perf_counter()
    rs.insert_data()
    toc = time.perf_counter()
    print(f"Inserted all of the data in {toc - tic:0.4f} seconds")

# deleting data from all of the tables

def deleting_time(rs):
    tic = time.perf_counter()
    rs.delete_all()
    toc = time.perf_counter()
    print(f"Deleted all of the data in {toc - tic:0.4f} seconds")

# getting the recommendation for given user
def recommendation_time(userid):
    tic = time.perf_counter()
    get_recommendations_mongo(userid)
    toc = time.perf_counter()
    print(f"Found recommendations for user in {toc - tic:0.4f} seconds")


# deleting ratings of some user
def delete_ratings_time(rs, userid):
    tic = time.perf_counter()
    rs.delete_user_ratings(userid)
    toc = time.perf_counter()
    print(f"Deleted ratings of a user in {toc - tic:0.4f} seconds")


# updating record of some user
def updating_time(rs, userid, isbn, rating):
    tic = time.perf_counter()
    rs.update_book_rating(userid, isbn, rating)
    toc = time.perf_counter()
    print(f"Updated rating of a user in {toc - tic:0.4f} seconds")



def time_all(rs):
    for _ in range(10):
        deleting_time(rs)
        inserting_time(rs)

    userid = 193560 #user with 849 ratigns
    userid2 = 191728  # user with 5 records
    userid3 = 11  #user without ratings


    for _ in range(5):
        recommendation_time(userid)

    for _ in range(3):
        recommendation_time(userid2)

    for _ in range(2):
        recommendation_time(userid3)

    isbn1= "0099850001"
    isbn2 = "0676972152"

    for n in range(5):
        updating_time(rs, userid, isbn1, n)

    for n in range(5):
        updating_time(rs, userid2, isbn2, n)


    userid_list = [193560, 191756, 250764, 91501, 91408, 276747, 276854, 78440, 247488, 156150]

    for userid in userid_list:
        delete_ratings_time(rs, userid)


