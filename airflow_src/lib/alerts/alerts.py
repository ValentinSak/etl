from lib.repository import execute_statement_as_dataframe
from lib.messangers.slack import send_message, get_message
import pandas as pd


def store_sales_alert(recipients: list[dict], threshold: float) -> None:
    '''Analyzes store sales data and sends alerts for stores with
    significant sales decreases

    params:
    recipients - list of dictionaries containing recipient information
    threshold - percentage threshold for sales decrease to trigger alert

    The function:
    1. Queries sales data for the last 7 days
    2. Calculates average sales and yesterday's sales for each store
    3. Identifies stores with sales decrease below the threshold
    4. Generates alert messages for affected stores
    '''
    query = '''
        WITH 
            sales_data AS (
                SELECT 
                    stores.name AS store_name,
                    products.price,
                    SUM(sales.quantity) AS total_quantity,
                    SUM(sales.quantity) * products.price AS total_amount,
                    sales.sale_date::DATE
                FROM etl.sales AS sales
                LEFT JOIN etl.orders AS orders ON sales.order_id = orders.id
                LEFT JOIN etl.products AS products ON sales.product_id = products.id
                LEFT JOIN etl.stores AS stores ON sales.store_id = stores.id
                WHERE sales.sale_date::DATE >= now()::DATE - INTERVAL '8 days'
                AND sales.sale_date::DATE < now()::DATE
                GROUP BY store_name, sales.sale_date, products.price
            ),
            aggregated_sales_data AS  (
                SELECT 
                    store_name,
                    AVG(
                        SUM(total_amount)
                        ) OVER (
                            PARTITION BY store_name 
                            ORDER BY sale_date 
                            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
                    ) AS seven_days_avg_amount,
                    (SELECT 
                        SUM(total_amount) FROM sales_data
                    WHERE 1 = 1
                    AND store_name = sd.store_name 
                    AND sale_date = now()::DATE - INTERVAL '1 day') AS yesterday_amount
                FROM sales_data sd
                GROUP BY store_name, sale_date
            )

            SELECT 
                store_name,
                seven_days_avg_amount,
                yesterday_amount,
                CASE
                    WHEN yesterday_amount IS NULL THEN -100
                    WHEN seven_days_avg_amount IS NULL THEN 0
                    ELSE (
                        (yesterday_amount - seven_days_avg_amount) 
                        / 
                        NULLIF(seven_days_avg_amount, 0) * 100)
                END AS percent_difference
            FROM aggregated_sales_data
    '''
    print(query)
    df = execute_statement_as_dataframe(query)

    if df.empty:
        return

    df = df[df['percent_difference'] <= threshold]

    df['message'] = df.apply(lambda row: get_store_sales_message(row), axis=1)

    for recipient in recipients:
        print(f'message was sent to {recipient["id"]}')

    # this is an example of creating message and sending it with slack

    # message = '/n'.join(df['message'].tolist())
    # slack_message = get_message(
    #     header='stores with',
    #     message=message
    # )

    # for recipient in recipients:
    #     send_message(recipient, slack_message)


def get_store_sales_message(row: pd.Series) -> str:
    '''Generates a formatted message for store sales alert

    params:
    row - pandas Series containing store sales data

    Returns a string message with store name and sales decrease percentage
    '''
    return f'Shop {row.store_name} sales decreased by {row.percent_difference}%'
