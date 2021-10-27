from pathlib import Path
import pandas as pd

from feast_sandbox.credit_tutorial.credit_model import CreditScoringModel

cur_dir_path = Path(__file__).absolute().parent


if __name__ == "__main__":
    # Get historic loan data
    loans = pd.read_parquet(cur_dir_path.parent.parent / "repos/credit_parquet_repo/loan_table.parquet")

    # Create model
    model = CreditScoringModel()

    # Train model (using Redshift for zipcode and credit history features)
    # if not model.is_model_trained():
    model.train(loans)

    # Make online prediction (using DynamoDB for retrieving online features)
    loan_request = {
        "zipcode": [76104],
        "dob_ssn": ["19630621_4278"],
        "person_age": [133],
        "person_income": [59000],
        "person_home_ownership": ["RENT"],
        "person_emp_length": [123.0],
        "loan_intent": ["PERSONAL"],
        "loan_amnt": [35000],
        "loan_int_rate": [16.02],
    }

    result = model.predict(loan_request)

    if result == 0:
        print("Loan approved!")
    elif result == 1:
        print("Loan rejected!")
