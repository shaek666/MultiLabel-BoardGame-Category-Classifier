from flask import Flask, render_template, request
from gradio_client import Client

app = Flask(__name__)
client = Client("nosttradamus/multilabel-boardgame-genre-classifier")

@app.route("/", methods=['GET', 'POST'])
def index():
    if request.method == "POST":
        try:
            input_text = request.form['text']
            output = predict_genres(input_text)
            print(f"Model output: {output}")  # Debug print

            if output and isinstance(output, dict) and "confidences" in output:
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
        print(f"Sending request with text: {input_text[:100]}...")
        result = client.predict(
            input_text,
            api_name="/predict"
        )
        print(f"Raw response type: {type(result)}")
        print(f"Raw response content: {result}")
        return result
    except Exception as e:
        print(f"Error in prediction: {str(e)}")
        import traceback
        print(f"Full traceback: {traceback.format_exc()}")
        return None

if __name__ == "__main__":
    app.run(debug=True)