from timing import time_all, inserting_time
from RecommendationSystem import RecommendationSystem
import RedisSystem as redisS
from RedisSystem import RedisRecommendationSystem, get_recommendations_redis

# for timing
# inserting all the records
# getting the recommendation for given user
# deleting the tables
# deleting ratings of some user
# updating record of some user

def compare_matrices(matrix1, matrix2):
   if len(matrix1) != len(matrix2) or len(matrix1[0]) != len(matrix2[0]):
      return False

   for i in range(len(matrix1)):
      for j in range(len(matrix1[0])):
         if matrix1[i][j] != matrix2[i][j]:
            return False

   return True


def sum_of_matrix(matrix):
   total_sum = 0
   for row in matrix:
      total_sum += sum(row)
   return total_sum

if __name__ == '__main__':
   #the Apache Cassandra system
   #rs = RecommendationSystem()
   #inserting_time(rs)
   #time_all(rs)

   #Redis system
   rrs = RedisRecommendationSystem()
   time_all(rrs)
   #rrs.insert_data()

   '''

   #rrs.insert_ratings_data()
   #rrs.insert_user_data()
   #rrs.insert_book_data()
   #r = rrs.get_general_recommendations()
   #print(r)
   #rrs.delete_user_ratings(276729)
   #rrs.get_rating_for_book(250750, '1567920950')
   #rrs.update_book_rating(179718,'380765489', 7.0)
   #rrs.get_rating_for_book(250758,'6545106')
   ids = {'156740', '38023', '120828', '131594', '263460', '60387', '41084', '152318', '3148', '184132', '219546', '91931', '240541', '99347', '250359'}
   ids2 = [156740, 38023, 120828, 131594, 263460, 60387, 41084, 152318, 3148, 184132, 219546, 91931, 240541, 99347, 250359]
   user_id = 250750
   best, worst = rrs.best_and_worst_books(user_id)
   print(best)
   print(worst)

   best2, worst2 = rs.best_and_worst_books(user_id)
   print(best2)
   print(worst2)



  
   user_item_matrix, unique_users, unique_books = rrs.create_user_item_matrix(ids)
   #print(unique_users)
   unique_users = [int(item) for item in unique_users]
   target_user_index = unique_users.index(user_id)

   book_indexes = redisS.book_recommendations_indexes(user_item_matrix, target_user_index)
   #print(book_indexes)
   recommended_isbn = []

   for book in book_indexes:
      recommended_isbn.append(str(unique_books[book]))
   #   print(str(unique_books[book]))
   recommendations = rrs.get_recommended_books(recommended_isbn)
   print(recommendations)
   #redisS.check_user(55)
   #redisS.retrieve_book_data("2005018")'''
