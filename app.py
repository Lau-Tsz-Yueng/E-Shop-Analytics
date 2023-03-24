from flask import Flask, jsonify, render_template
from pre_processed_data import Statistics
from datetime import date

app = Flask(__name__)
statistics = Statistics()


@app.route("/api/statistics/<date_string>", methods=["GET"])
def get_statistics(date_string):
    """Endpoint to retrieve statistics for a given date.

    Args:
        date_string (str): The date in format 'YYYY-MM-DD' for which to retrieve the statistics.

    Returns:
        A JSON object containing the statistics for the requested date, or a custom error message if no data is available.
    """
    response = statistics.get_statistics_for_date(date_string)
    if "error" in response:
        return jsonify(response), 404
    elif not response:
        error_message = {"error": f"No data available for date {date_string}"}
        return jsonify(error_message), 404
    return jsonify(response)


@app.route("/")
def get_statistics_form():
    """Endpoint to render the HTML form for requesting statistics."""
    today = date.today().strftime('%Y-%m-%d')
    return render_template("get_statistics.html", date=today)


if __name__ == "__main__":
    # Set debug mode to False for production deployment
    app.run(debug=False)
