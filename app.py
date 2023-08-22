from flask import Flask, render_template, request
from RecommendationSystem import get_recommendations
from RedisSystem import get_recommendations_redis
from MongoDBSystem import get_recommendations_mongo
from Neo4jSystem import get_recommendations_neo4j

app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def input_form():
    if request.method == 'POST':
        user_input = request.form['user_input']
        books = get_recommendations_neo4j(user_input)
        if books == -1:
            return render_template('input.html', error = 1)
        return render_template('books.html', book_data=books)
    return render_template('input.html')

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5001)