import joblib

def get_full_class():
    label_encoder = joblib.load('model/xgboostEncoder.joblib')
    full_classes = label_encoder.classes_
    return full_classes

if __name__ == "__main__":
    full_class = get_full_class()
    print(full_class)