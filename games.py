import warnings
import pandas as pd
from datetime import timedelta
from datetime import datetime
from airflow.decorators import dag, task

# Отключаем предупреждения
warnings.filterwarnings("ignore")

# Задаем значения по умолчанию для DAG
default_args = {
    'owner': 'mi_sozonov',  # Указываем владельца DAG
    'depends_on_past': False,  # Указываем, зависит ли DAG от предыдущих запусков
    'retries': 2,  # Указываем количество попыток в случае ошибки
    'retry_delay': timedelta(minutes=5),  # Указываем задержку между попытками
    'start_date': datetime(2021, 10, 7)  # Указываем дату начала запуска
}

# Вычисляем год для анализа данных
# Используем хэш для получения уникального года
my_date = 1994 + hash(f'mi-sozonov') % 23

# Определяем DAG


@dag(default_args=default_args, schedule_interval='0 15 * * *', catchup=False, tags=['mi_sozonov'])
def gaming_sales_mi_sozonov():
    # Определяем задачу для загрузки данных
    # Указываем количество попыток и задержку для задачи
    @task(retries=4, retry_delay=timedelta(10))
    def get_data(my_date):
        # Загружаем данные из CSV-файла
        link = "https://kc-course-static.hb.ru-msk.vkcs.cloud/startda/Video%20Game%20Sales.csv"
        data = pd.read_csv(link)
        # Фильтруем данные по заданному году
        data = data[data['Year'] == my_date]
        return data

    # Определяем задачу для поиска самой продаваемой игры
    @task()
    def top_sales_game(data):
        """ 
        Какая игра была самой продаваемой в этом году во всем мире? 
        """
        # Группируем данные по названию игры и суммируем продажи по всему миру
        most_sold_game = data.groupby('Name')['Global_Sales'].sum().idxmax()
        return most_sold_game

    # Определяем задачу для поиска самых продаваемых жанров в Европе
    @task()
    def top_sales_game_eu(data):
        """ 
        Игры какого жанра были самыми продаваемыми в Европе? 
        Перечислить все, если их несколько 
        """
        # Группируем данные по жанру и суммируем продажи в Европе
        most_sold_game_eu = data.groupby('Genre')['EU_Sales'].sum()
        # Находим максимальное значение продаж и получаем список жанров с максимальными продажами
        most_sold_game_eu = most_sold_game_eu[most_sold_game_eu == most_sold_game_eu.max(
        )].index.tolist()
        # Возвращаем список жанров, разделенных запятой
        return ", ".join(most_sold_game_eu)

    # Определяем задачу для поиска платформ с наибольшим количеством игр, проданных более чем миллионным тиражом в Северной Америке
    @task()
    def more_one_mill(data):
        '''
        На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
        Перечислить все, если их несколько
        '''
        # Фильтруем данные по продажам в Северной Америке, больше 1 миллиона
        top_platforms = data.query('NA_Sales > 1').groupby(
            'Platform')['Year'].count()
        # Находим платформы с максимальным количеством игр
        top_platforms = top_platforms[top_platforms == top_platforms.max()].reset_index()[
            'Platform']
        # Возвращаем список платформ, разделенных запятой
        return ", ".join(top_platforms.astype(str))

    # Определяем задачу для поиска издателя с самыми высокими средними продажами в Японии
    @task()
    def sales_jp(data):
        """ 
        У какого издателя самые высокие средние продажи в Японии?
        Перечислить все, если их несколько 
        """
        # Группируем данные по издателю и вычисляем средние продажи в Японии
        avg_sales_jp = data.groupby('Publisher')['JP_Sales'].mean()
        # Находим максимальное значение средних продаж
        max_avg_sales = avg_sales_jp.max()
        # Получаем список издателей с максимальными средними продажами
        top_publishers = avg_sales_jp[avg_sales_jp ==
                                      max_avg_sales].index.tolist()
        # Возвращаем список издателей, разделенных запятой
        return ", ".join(top_publishers)

    # Определяем задачу для подсчета количества игр, проданных лучше в Европе, чем в Японии
    @task()
    def more_eu_jp(data):
        """ 
        Сколько игр продались лучше в Европе, чем в Японии? 
        """
        # Фильтруем данные по продажам в Европе, больше, чем в Японии
        more_eu_than_jp = data[data['EU_Sales'] > data['JP_Sales']].shape[0]
        return more_eu_than_jp

    # Определяем задачу для вывода результатов
    @task()
    def print_data(top_sales_game_task, top_sales_game_eu_task, more_one_mill_task, sales_jp_task, more_eu_jp_task):
        try:
            print(
                f"Самая продаваемая игра в мире в {my_date} году: {top_sales_game_task}")
            print(
                f"Самые продаваемые жанры в Европе в {my_date} году: {top_sales_game_eu_task}")
            print(
                f"Платформы с наибольшим количеством игр, проданных более чем миллионным тиражом в Северной Америке в {my_date} году: {more_one_mill_task}")
            print(
                f"Издатель с самыми высокими средними продажами в Японии в {my_date} году: {sales_jp_task}")
            print(
                f"Количество игр, проданных лучше в Европе, чем в Японии в {my_date} году: {more_eu_jp_task}")
        except Exception as e:
            # Обрабатываем ошибки при выводе результатов
            print(f"Ошибка при печати данных: {e}")

    # Запускаем задачу загрузки данных
    data = get_data(my_date)

    # Запускаем задачи анализа данных
    top_sales_game_task = top_sales_game(data)
    top_sales_game_eu_task = top_sales_game_eu(data)
    more_one_mill_task = more_one_mill(data)
    sales_jp_task = sales_jp(data)
    more_eu_jp_task = more_eu_jp(data)

    print_data(top_sales_game_task, top_sales_game_eu_task,
               more_one_mill_task, sales_jp_task, more_eu_jp_task)


# Запускаем DAG
gaming_sales_mi_sozonov = gaming_sales_mi_sozonov()
