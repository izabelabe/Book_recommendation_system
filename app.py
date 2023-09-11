from flask import Flask, render_template, request, redirect, url_for
from RecommendationSystem import get_recommendations, addUser, addRating
from RedisSystem import get_recommendations_redis, addUserRedis, addRatingRedis
from MongoDBSystem import get_recommendations_mongo, addUserMongo, addRatingMongo
from Neo4jSystem import get_recommendations_neo4j, addUserNeo4j, addRatingNeo4j

app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def input_form():
    if request.method == 'POST':
        user_input = request.form['user_input']
        books = get_recommendations(user_input)
        if books == -1:
            return render_template('input.html', error = 1)
        return render_template('books.html', book_data=books, userid = user_input)
    return render_template('input.html')

@app.route('/books', methods=['GET'])
def books():
    userid = request.args.get('userid')
    books = get_recommendations(userid)
    return render_template('books.html', book_data=books, userid=userid)

@app.route('/SignUp', methods=['GET','POST'])
def user_input_form():
    if request.method == 'POST':
        userid = request.form['userid_input']
        location = request.form['location_input']
        age = request.form['age_input']
        result = addUser(int(userid), location, age)
        if result == -1:
            return render_template('signUp.html', error = 1)
        return render_template('signUp.html', error= 2)
    return render_template('signUp.html')

@app.route('/addBooks', methods=['GET','POST'])
def addBooks():
    userid = request.args.get('userid')
    if request.method == 'POST':
        book = request.form['isbn_input']
        rating = request.form['rating_input']
        result = addRating(int(userid), book, rating)
        if result == -1:
            return render_template('bookAddition.html', userid=userid, error = 1)
        elif result == -2:
            return render_template('bookAddition.html', userid=userid, error=2)
        return render_template('bookAddition.html', userid=userid)
    return render_template('bookAddition.html', userid=userid)


#if __name__ == '__main__':
#    app.run(host='127.0.0.1', port=5001)