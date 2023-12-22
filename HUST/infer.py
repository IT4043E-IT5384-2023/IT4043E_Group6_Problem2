import torch
from transformers import AutoModel, AutoTokenizer, AutoConfig
import joblib
import numpy as np
import xgboost as xgb
from utils import load_config

def get_bert_model(cfg):
    model_name = cfg["bert_name"]
    bert_model = AutoModel.from_pretrained(model_name)
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    for param in bert_model.parameters():
        param.requires_grad = False
    
    config = AutoConfig.from_pretrained(model_name)
    # max_length = config.max_position_embeddings
    return bert_model, tokenizer, config

def get_encoder(cfg):
    label_encoder = joblib.load(cfg["encoder_path"])
    return label_encoder

def get_xgb_model(cfg):
    loaded_model = xgb.Booster()
    loaded_model.load_model(cfg["model_path"])
    return loaded_model

def infer(data, cfg):
    bert_model, tokenizer, config = get_bert_model(cfg)
    xg_model = get_xgb_model(cfg)
    label_encoder = get_encoder(cfg)
    features = []
    for i in range(0, len(data), 2):
        batch_sentences = data[i:i+2]
        tokenized_inputs = tokenizer(batch_sentences, return_tensors="pt", padding=True, truncation=True)
        with torch.no_grad():
            outputs = bert_model(**tokenized_inputs)
            reshaped_output = outputs.pooler_output.view(outputs.pooler_output.size(0), -1, 8)
            mean_output = reshaped_output.mean(dim=2)
        for feature in mean_output:
            features.append(feature.numpy())
    print(len(features))
    dMatrix = xgb.DMatrix(features)
    y_pred_prob = xg_model.predict(dMatrix)
    top_3_pred_indices = np.argsort(y_pred_prob, axis=1)[:, -3:]
    top_3_indices = np.fliplr(top_3_pred_indices)
    top_3_labels = label_encoder.inverse_transform(top_3_indices.flatten()).reshape(top_3_indices.shape)
    return top_3_labels

if __name__ == "__main__":
    cfg = load_config("./config/model.yaml")
    data = ["Gaming", "Testing", "Testing","Testing","Testing","Testing",
            "Gaming", "Testing", "Testing","Testing","Testing","Testing",
            "Gaming", "Testing", "Testing","Testing","Testing","Testing",
            "Gaming", "Testing", "Testing","Testing","Testing","Testing",
            "Gaming", "Testing", "Testing","Testing","Testing","Testing"]
    print(infer(data, cfg))
    pass