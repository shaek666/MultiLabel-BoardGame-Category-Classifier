from flask import Flask, render_template, request
from gradio_client import Client

app = Flask(__name__)
client = Client("https://nosttradamus-multilabel-boardgame-genre-classifier.hf.space/")

@app.route("/", methods=['GET', 'POST'])
def index():
    if request.method == "POST":
        try:
            input_text = request.form['text']
            output = predict_genres(input_text)

            if output and isinstance(output, dict) and "confidences" in output and output['confidences'] is not None:
                try:
                    # Get genres with confidence >= 0.2
                    labels = [item['label'] for item in output['confidences'] if float(item.get('confidence', 0)) >= 0.2]
                    label_text = ", ".join(labels) if labels else "No genres predicted"
                    return render_template("result.html", input_text=input_text, output_text=label_text)
                except (KeyError, ValueError, TypeError) as e:
                    return render_template("result.html", input_text=input_text, 
                        output_text=f"Error processing model response: {str(e)}")
            else:
                return render_template("result.html", input_text=input_text, 
                    output_text=f"Error: Invalid response from model. Got: {str(output)}")
        except Exception as e:
            return render_template("result.html", input_text=input_text, output_text=f"Error: {str(e)}")
    return render_template("index.html")

def predict_genres(input_text):
    try:
        result = client.predict(input_text, api_name="/predict")
        # Check if the expected data is in the result.
        if isinstance(result, dict) and 'confidences' in result:
            return result
        else:
            # Log or handle unexpected API response structure
            return None
    except Exception as e:
        return None

if __name__ == "__main__":
    app.run(debug=True)