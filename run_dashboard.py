from data_loader import fetch_flights, load_to_db
from dashboard import run_app

api_key1 = 'your_api_key'
api_key2 = 'your_api_key'
new_db_name = 'your_db_name'

def run():

    print("Скачивание данных...")
    df = fetch_flights(api_key1, api_key2)

    print("Загрузка в базу...")
    load_to_db(df, db_name=new_db_name)

    print("Запуск дашборда...")
    app = run_app(new_db_name)
    app.run(debug=False)

if __name__ == "__main__":
    run() 