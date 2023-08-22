from timing import time_all, inserting_time
from RecommendationSystem import RecommendationSystem
import RedisSystem as redisS
from RedisSystem import RedisRecommendationSystem, get_recommendations_redis
from pymongo import MongoClient
from MongoDBSystem import MongoRecommendationSystem, get_recommendations_mongo
from Neo4jSystem import Neo4jRecommendationSystem, get_recommendations_neo4j
import pandas as pd



#if __name__ == '__main__':

#   recommendation_system = Neo4jRecommendationSystem()
#   time_all(recommendation_system)


