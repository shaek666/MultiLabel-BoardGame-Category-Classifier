# 🎲 Board Game Genre Classifier 🎲

This web application leverages a machine learning model to predict the genres of a board game based on its description. Describe your game concept, and the AI will provide a list of predicted genres, helping you understand its potential market placement and core mechanics.

## ✨ Features

- **AI-Powered Genre Prediction**: Utilizes a fine-tuned model to classify board game descriptions into multiple genres.
- **Interactive UI**: A clean and engaging user interface for submitting game descriptions.
- **Responsive Design**: The application is designed to work on both desktop and mobile devices.
- **Dynamic Backgrounds**: Features animated cards, tokens, and dice to create an immersive experience.

## 🛠️ Technologies Used

- **Backend**: Python, Flask
- **Frontend**: HTML, CSS, JavaScript
- **Machine Learning**: Hugging Face Transformers

## 🚀 Getting Started

### Prerequisites

- Python 3.7+
- `pip` for package management

### Installation

1.  **Clone the repository:**
    ```bash
    git clone <repository-url>
    cd MultiLabel-BoardGame-Category-Classifier
    ```

2.  **Create a virtual environment:**
    ```bash
    python -m venv env
    source env/bin/activate  # On Windows, use `env\Scripts\activate`
    ```

3.  **Install the dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

### Running the Application

1.  **Start the Flask server:**
    ```bash
    python app.py
    ```

2.  Open your browser and navigate to `http://127.0.0.1:5000` to use the application.