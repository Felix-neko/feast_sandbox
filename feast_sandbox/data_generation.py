
import numpy as np
import numpy.random

if __name__ == "__main__":

    n_users = 50
    n_rows = 200

    max_chunk_size = 39

    rows_generated = 0

    while rows_generated <= n_rows:
        min_row = rows_generated
        max_row = min(n_rows, rows_generated + max_chunk_size)
        chunk_size = max_row - min_row + 1

        min_uid, max_uid = int(n_users * min_row / n_rows), int(n_users * max_row / n_rows)
        print(f"min_row: {min_row}, max_row: {max_row},\tmin_uid: {min_uid}, max_uid: {max_uid}")

        rows_generated += chunk_size



    # unique_users_places = np.random.choice(n_rows, n_users, replace=False)
    # print(uids)