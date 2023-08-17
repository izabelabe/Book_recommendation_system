from timing import time_all, inserting_time
from RecommendationSystem import RecommendationSystem
import RedisSystem as redisS
from RedisSystem import RedisRecommendationSystem, get_recommendations_redis
from pymongo import MongoClient
from MongoDBSystem import MongoRecommendationSystem, get_recommendations_mongo


if __name__ == '__main__':

   #the Apache Cassandra system
   #rs = RecommendationSystem()
   #inserting_time(rs)
   #time_all(rs)

   #Redis system
   #rrs = RedisRecommendationSystem()
   #time_all(rrs)
   #rrs.insert_data()

   mg = MongoRecommendationSystem()
   time_all(mg)
   mg.client.close()
   #mg.update_book_rating(276726, "0155061224", 10)
   #mg.delete_all()
   #mg.insert_data()
   #books = get_recommendations_mongo(193560)
   #print(books)
   #mg.client.close()