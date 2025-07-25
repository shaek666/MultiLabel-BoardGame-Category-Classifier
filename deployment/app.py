import gradio as gr
import onnxruntime as rt
from transformers import AutoTokenizer
import torch, json

tokenizer = AutoTokenizer.from_pretrained("distilroberta-base")

with open("category_types_encoded.json", "r") as fp:
  encode_category_types = json.load(fp)

categories = list(encode_category_types.keys())

inf_session = rt.InferenceSession('game-classifier-quantized.onnx')
input_name = inf_session.get_inputs()[0].name
output_name = inf_session.get_outputs()[0].name

def classify_game_category(description):
  input_ids = tokenizer(description)['input_ids'][:512]
  logits = inf_session.run([output_name], {input_name: [input_ids]})[0]
  logits = torch.FloatTensor(logits)
  probs = torch.sigmoid(logits)[0]
  return dict(zip(categories, map(float, probs))) 

label = gr.Label(num_top_classes=5)
iface = gr.Interface(fn=classify_game_category, inputs=gr.Textbox(), outputs=label)
iface.launch()
					