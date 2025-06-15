import joblib

import numpy as np
import pandas as pd

model = joblib.load('./models/rf_play_call_model.pkl')
label_encoder = joblib.load('./models/play_type_label_encoder.pkl')

def predict_play_type_explicit(
    quarter,
    game_seconds_remaining,
    posteam_score,
    defteam_score,
    down,
    ydstogo,
    yardline_100,
    posteam_timeouts_remaining,
    defteam_timeouts_remaining,
    is_clock_running
):
    """
    Predicts play_type given explicit play scenario values.
    Returns the predicted play_type string.
    """
    
    data = pd.DataFrame([{
        'quarter': quarter,
        'game_seconds_remaining': game_seconds_remaining,
        'posteam_score': posteam_score,
        'defteam_score': defteam_score,
        'down': down,
        'ydstogo': ydstogo,
        'yardline_100': yardline_100,
        'posteam_timeouts_remaining': posteam_timeouts_remaining,
        'defteam_timeouts_remaining': defteam_timeouts_remaining,
        'is_clock_running': int(is_clock_running)  # convert bool to int
    }])

    prediction_encoded = model.predict(data)
    prediction_label = label_encoder.inverse_transform(prediction_encoded)

    return prediction_label[0]

def predict_play_type_probabilities(
    quarter,
    game_seconds_remaining,
    posteam_score,
    defteam_score,
    down,
    ydstogo,
    yardline_100,
    posteam_timeouts_remaining,
    defteam_timeouts_remaining,
    is_clock_running
):
    """
    Predicts the probability distribution over play_types given explicit play scenario values.
    Returns a dictionary of {play_type: probability}.
    """

    data = pd.DataFrame([{
        'quarter': quarter,
        'game_seconds_remaining': game_seconds_remaining,
        'posteam_score': posteam_score,
        'defteam_score': defteam_score,
        'down': down,
        'ydstogo': ydstogo,
        'yardline_100': yardline_100,
        'posteam_timeouts_remaining': posteam_timeouts_remaining,
        'defteam_timeouts_remaining': defteam_timeouts_remaining,
        'is_clock_running': int(is_clock_running)  # convert bool to int
    }])

    # Get probability estimates
    probabilities = model.predict_proba(data)[0]  # Extract the 1D array
    class_labels = label_encoder.inverse_transform(np.arange(len(probabilities)))

    # Combine labels with their probabilities into a dict
    return dict(zip(class_labels, probabilities))


def generate_clutch_scenarios(n=10, seed=42, fixed_down=None):
    """
    Generate n random clutch game scenarios (e.g., under 5 minutes in Q4).
    If fixed_down is provided, all plays will be that down.
    """
    np.random.seed(seed)

    posteam_score = np.random.randint(14, 35, size=n)
    score_diff = np.random.randint(-10, 11, size=n)
    defteam_score = np.clip(posteam_score + score_diff, 0, None)

    return pd.DataFrame({
        'quarter': [4] * n,
        'game_seconds_remaining': np.random.randint(1, 300, size=n),
        'posteam_score': posteam_score,
        'defteam_score': defteam_score,
        'down': fixed_down if fixed_down else np.random.randint(1, 5, size=n),
        'ydstogo': np.random.randint(1, 20, size=n),
        'yardline_100': np.random.randint(1, 99, size=n),
        'posteam_timeouts_remaining': np.random.randint(0, 4, size=n),
        'defteam_timeouts_remaining': np.random.randint(0, 4, size=n),
        'is_clock_running': np.random.randint(0, 2, size=n)
    })

def predict_scenario_df(df):
    """
    Predict play types for a DataFrame of game scenarios.
    Returns the DataFrame with an added 'predicted_play_type' column.
    """
    df = df.copy()
    df['is_clock_running'] = df['is_clock_running'].astype(int)
    encoded_preds = model.predict(df)
    df['predicted_play_type'] = label_encoder.inverse_transform(encoded_preds)
    return df

# Example usage
if __name__ == '__main__':
    
    # scenarios = generate_clutch_scenarios(n=100, fixed_down=4)
    # predictions = predict_scenario_df(scenarios)
    predictions = predict_play_type_probabilities(
        quarter=4,
        game_seconds_remaining=60,
        posteam_score=21,
        defteam_score=21,
        down=4,
        ydstogo=1,
        yardline_100=1,
        posteam_timeouts_remaining=0,
        defteam_timeouts_remaining=0,
        is_clock_running=False
    )

    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    print(predictions)