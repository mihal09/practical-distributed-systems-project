from flask import Flask

app = Flask(__name__)

@app.route('/test', methods=['GET'])
def test():
    # Here you would handle the user tag
    print(f"Testing.")
    return "TEST"


if __name__ == '__main__':
    app.run(debug=True, port=8000, host="0.0.0.0")
