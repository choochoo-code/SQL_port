"""
SQL Data Transform Service - Main entry point
"""
from flask import Flask, render_template

from app.routes.merge import merge_bp
from app.routes.resample import resample_bp

app = Flask(__name__)
app.secret_key = "your-secret-key-change-in-production"

# Register blueprints
app.register_blueprint(merge_bp)
app.register_blueprint(resample_bp)


@app.route('/')
def index():
    return render_template('index.html')


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
