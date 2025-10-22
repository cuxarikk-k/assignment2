import random
import mysql.connector
from faker import Faker
from typing import Tuple

# --- Configuration ---
DB_CONFIG = {
    'host': '127.0.0.1',
    'user': 'root',
    'password': 'MySQL_Student123',
    'database': 'assignment2'
}
TOTAL_COCKTAILS = 1_000_000
TOTAL_INGREDIENTS = 1_000_000
TOTAL_RECIPE_USAGE = 3_000_000
BATCH_SIZE = 50_000
fake = Faker()


# --- insertion ---
def execute_batch_insert(connection, total_rows, batch_size, table_name, insert_query, record_generator):
    print(f"\n start of insertion in {table_name} ({total_rows} rows)...")
    cursor = connection.cursor()
    current_id = 1

    for _ in range(0, total_rows, batch_size):
        batch_end_id = min(current_id + batch_size, total_rows + 1)
        batch = [record_generator(cid) for cid in range(current_id, batch_end_id)]

        cursor.executemany(insert_query, batch)
        connection.commit()
        current_id = batch_end_id
        print(f"  inserted {current_id - 1} / {total_rows} rows")

    cursor.close()
    print(f"✅ {table_name}: data insertion complete!")


# --- Table Generators ---
def generate_cocktail_record(cocktail_id: int) -> Tuple:
    name = fake.sentence(nb_words=4).replace('.', '').strip()
    category = random.choice(('tropical', 'classic', 'modern', 'sweet', 'sour'))
    base = random.choice(('Gin', 'Horilka', 'Rum', 'Tequila', 'Whiskey'))
    difficulty = random.randint(1, 5)
    popularity = round(random.uniform(3.0, 5.0), 2)
    prep_date = fake.date_between(start_date='-5y', end_date='today')
    return (cocktail_id, name, category, base, difficulty, popularity, prep_date)


def generate_ingredient_record(ingredient_id: int) -> Tuple:
    name = fake.word() + " " + fake.color_name()
    quality = round(random.uniform(3.0, 5.0), 2)
    ingredient_type = random.choice(('syrop', 'juice', 'garnish', 'spice', 'alcohol', 'soda'))
    cost_usd = round(random.uniform(0.1, 50.0), 2)
    quantity = random.randint(10, 1000)
    return (ingredient_id, name, quality, ingredient_type, cost_usd, quantity)


def generate_recipe_usage_record(usage_id: int) -> Tuple:
    cocktail_id = random.randint(1, TOTAL_COCKTAILS)
    ingredient_id = random.randint(1, TOTAL_INGREDIENTS)
    volumeMl = random.randint(5, 500)
    usage_date = fake.date_between(start_date='-5y', end_date='today')
    return (usage_id, cocktail_id, ingredient_id, volumeMl, usage_date)

def insert_all_data(connection: mysql.connector.CMySQLConnection) -> None:
    # cocktails
    cocktail_query = "INSERT INTO cocktails (cocktail_ID, name, category, base, difficulty_score, popularity_score, preparation_date) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    execute_batch_insert(connection, TOTAL_COCKTAILS, BATCH_SIZE, "cocktails", cocktail_query, generate_cocktail_record)

    # ingredients
    ingredient_query = "INSERT INTO ingredients (ingredient_id, name, quality_score, type, cost_usd, quantity) VALUES (%s, %s, %s, %s, %s, %s)"
    execute_batch_insert(connection, TOTAL_INGREDIENTS, BATCH_SIZE, "ingredients", ingredient_query,
                         generate_ingredient_record)

    # recipeUsage
    usage_query = "INSERT INTO recipeUsage (usage_id, cocktail_id, ingredient_id, volumeMl, usage_date) VALUES (%s, %s, %s, %s, %s)"
    execute_batch_insert(connection, TOTAL_RECIPE_USAGE, BATCH_SIZE, "recipeUsage", usage_query,
                         generate_recipe_usage_record)


def generate_and_insert_all_data() -> None:
    connection = None
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        if connection.is_connected():
            print("database connection established!")
            insert_all_data(connection)

    except mysql.connector.Error as err:
        print(f"error MySQL: {err}")
    finally:
        if connection and connection.is_connected():
            connection.close()
            print("\nconnection closed.")


if __name__ == "__main__":
    generate_and_insert_all_data()