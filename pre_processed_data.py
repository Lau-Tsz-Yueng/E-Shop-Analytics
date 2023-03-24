import os
import sqlite3
import pandas as pd


class DataTableReader:
    """
    Reads a CSV file, converts it into a pandas DataFrame, and stores it in a SQLite database.
    """

    def __init__(self, csv_file, renamed_table_name):
        self.data_frame = self.load_csv_to_dataframe(csv_file)
        self.create_database_and_save_table(csv_file, renamed_table_name)

    @classmethod
    def load_csv_to_dataframe(cls, csv_file):
        """
        Loads data from a CSV file into a pandas DataFrame.
        """
        return pd.read_csv(csv_file)

    def create_database_and_save_table(self, csv_file, renamed_table_name):
        """
        Creates a SQLite database and saves the DataFrame as a table.
        """
        db_folder = os.path.join(os.getcwd(), 'databases')
        os.makedirs(db_folder, exist_ok=True)

        db_name = os.path.splitext(os.path.basename(csv_file))[0] + '.db'
        db_file_path = os.path.join(db_folder, db_name)

        with sqlite3.connect(db_file_path) as connection:
            self.data_frame.to_sql(renamed_table_name, connection, if_exists='replace', index=False)


class TableSaver:
    """
    Iterates through a directory and its subdirectories, processes CSV files, and saves them as tables in SQLite databases.
    """

    def __init__(self):
        self.directory_path = os.getcwd()

    def iterate_csv_files(self, directory):
        """
        Iterates through a directory and processes CSV files.
        """
        directory_path = os.path.join(self.directory_path, directory)

        for root, _, files in os.walk(directory_path):
            for file_name in files:
                file_path = os.path.join(root, file_name)
                renamed_file_name = os.path.splitext(file_name)[0]
                DataTableReader(file_path, renamed_file_name)


class Statistics:
    """
    Loads data from CSV files into pandas DataFrames for further processing and analysis.

    Statistics for below metrics for a certain data:
    {
        The total number of items sold on that day.
        The total number of customers that made an order that day.
        The total amount of discount given that day.
        The average discount rate applied to the items sold that day.
        The average order total for that day
        The total amount of commissions generated that day.
        The average amount of commissions per order for that day.
        The total amount of commissions earned per promotion that day.
    }
    """

    ORDERS_FILE = "data/orders.csv"
    ORDER_LINES_FILE = "data/order_lines.csv"
    COMMISSIONS_FILE = "data/commissions.csv"
    PRODUCT_PROMOTIONS_FILE = "data/product_promotions.csv"
    PRODUCTS_FILE = "data/products.csv"
    PROMOTIONS_FILE = "data/promotions.csv"

    def __init__(self):
        self.orders = pd.read_csv(self.ORDERS_FILE)
        self.order_lines = pd.read_csv(self.ORDER_LINES_FILE)
        self.commissions = pd.read_csv(self.COMMISSIONS_FILE)
        self.product_promotions = pd.read_csv(self.PRODUCT_PROMOTIONS_FILE)
        self.products = pd.read_csv(self.PRODUCTS_FILE)
        self.promotions = pd.read_csv(self.PROMOTIONS_FILE)

        # Join the tables based on the id column in orders and the order_id column in order_lines
        self.merged_order_df = pd.merge(self.orders, self.order_lines, left_on='id', right_on='order_id')

        # Convert the 'created_at' column to a datetime object and extract the date
        self.merged_order_df['date'] = pd.to_datetime(self.merged_order_df['created_at']).dt.date

        # Convert the date columns to datetime data types
        self.merged_order_df['date'] = pd.to_datetime(self.merged_order_df['date'])
        self.commissions['date'] = pd.to_datetime(self.commissions['date'])
        self.product_promotions['date'] = pd.to_datetime(self.product_promotions['date'])

        # Merge the merged_df DataFrame with the commissions DataFrame on the 'vendor_id' column
        self.merged_commissions = pd.merge(self.merged_order_df, self.commissions, on=['vendor_id', 'date'])

        # Calculate the commission for each row and store it in a new column called 'commission'
        self.merged_commissions['commission'] = self.merged_commissions['total_amount'] * self.merged_commissions['rate']

    def total_number_of_items_sold_each_day(self):
        """
        Calculate the total number of items sold on each day by summing the quantity column in the OrderLine table
        for all orders created on that day. Returns a DataFrame with two columns: date and total_items_sold.
        """
        # Group by date and calculate the total number of items sold on each day
        items_sold_per_day = self.merged_order_df.groupby('date')['quantity'].sum().reset_index()

        # Display the result
        return items_sold_per_day

    def number_of_unique_customers_each_day(self):
        """
        Calculate the number of unique customers on each day by counting the distinct customer IDs in the merged
        DataFrame. Returns a Series with the number of unique customers per day, indexed by date.
        """
        # Group by date and calculate the number of unique customers on each day
        customers_per_day = self.merged_order_df.groupby('date')['customer_id'].nunique()

        # Display the result
        return customers_per_day

    def total_amount_of_discount_each_day(self):
        """
        Computes the total amount of discount given on each day by grouping the merged_order_df by date and summing the
        discounted_amount for each group.
        :return: a DataFrame containing the date and the total amount of discount given on that day.
        """
        # Group by date and calculate the total amount of discount given on each day
        discount_per_day = self.merged_order_df.groupby('date')['discounted_amount'].sum().reset_index()

        # Display the result
        return discount_per_day

    def average_discount_rate_per_day(self):
        """
        Calculates the average discount rate applied to the items sold on each day by grouping the merged_order_df by date
        and computing the mean of the discount_rate column for each group.
        :return: a DataFrame containing the date and the average discount rate applied to the items sold on that day.
        """
        # Group by date and calculate the average discount rate applied to the items sold on each day
        average_discount_rate_per_day = self.merged_order_df.groupby('date')['discount_rate'].mean().reset_index()

        return average_discount_rate_per_day

    def average_order_total(self):
        """
        Calculates the average order total for each day by grouping the merged_order_df by date and order_id, summing the
        total_amount for each group and computing the mean of the resulting DataFrame for each day.
        :return: a DataFrame containing the date and the average order total for that day.
        """
        # Group by date and calculate the average order total for each day
        average_order_total_per_day = \
            self.merged_order_df.groupby(['date', 'order_id'])['total_amount'].sum().reset_index().groupby('date')[
                'total_amount'].mean().reset_index()

        return average_order_total_per_day

    def total_commissions_per_day(self):
        """
        Calculates the total commission generated on each day by grouping the merged_commissions DataFrame by date and
        summing the commission for each group.
        :return: a DataFrame containing the date and the total commission generated on that day.
        """
        # Group by date and calculate the total commission generated on each day
        total_commissions_per_day = self.merged_commissions.groupby('date')['commission'].sum().reset_index()

        return total_commissions_per_day

    def average_amount_of_commissions_per_order(self):
        """
        Calculates the average commission per order for each day by grouping the merged_commissions DataFrame by date and
        order_id, summing the commission for each group, and then computing the mean of the resulting DataFrame for each day.
        :return: a DataFrame containing the date and the average commission per order for that day.
        """
        # Group by date and order_id, and calculate the total commission per order
        total_commissions_per_order = self.merged_commissions.groupby(['date', 'order_id'])['commission'].sum().reset_index()

        # Group by date and calculate the average commission per order for each day
        average_commissions_per_order_per_day = total_commissions_per_order.groupby('date')['commission'].mean().reset_index()

        return average_commissions_per_order_per_day

    def total_amount_of_commissions_earned_per_promotions(self):
        """
        Calculates the total commission earned per promotion on each day by merging the merged_commissions DataFrame with
        the product_promotions DataFrame on the 'product_id' and 'date' columns, grouping the resulting DataFrame by date
        and promotion_id, and summing the commission for each group.
        :return: a DataFrame containing the date, the promotion_id, and the total commission earned for that promotion on
        that day.
        """
        # Merge the merged_commissions DataFrame with the product_promotions DataFrame on the 'product_id' and 'date'
        # columns
        merged_promotions = pd.merge(self.merged_commissions, self.product_promotions, on=['product_id', 'date'])

        # Group by date and promotion_id, and calculate the total commission per promotion on each day
        total_commissions_per_promotion_per_day = merged_promotions.groupby(['date', 'promotion_id'])[
            'commission'].sum().reset_index()

        return total_commissions_per_promotion_per_day

    def get_statistics_for_date(self, date_string):
        """
        Returns a dictionary containing statistics for a specific date, including the number of unique customers, the total
        amount of discount given, the total number of items sold, the average order total, the average discount rate, and the
        commissions information (total commissions, average commissions per order, and commissions earned per promotion).
        :param date_string: a string representing the date for which to retrieve the statistics, in 'YYYY-MM-DD' format.
        :return: a dictionary containing the statistics for the specified date.
        """
        date = pd.to_datetime(date_string)

        # Get the number of unique customers for the specified date
        customers = self.number_of_unique_customers_each_day().loc[date]

        # Get the total amount of discount given for the specified date
        total_discount_amount = self.total_amount_of_discount_each_day().set_index('date').loc[
            date, 'discounted_amount']

        # Get the total number of items sold for the specified date
        items = self.total_number_of_items_sold_each_day().set_index('date').loc[date, 'quantity']

        # Get the average order total for the specified date
        order_total_avg = self.average_order_total().set_index('date').loc[date, 'total_amount']

        # Get the average discount rate for the specified date
        discount_rate_avg = self.average_discount_rate_per_day().set_index('date').loc[date, 'discount_rate']

        # Get the total commissions earned per promotion for the specified date
        promotions = self.total_amount_of_commissions_earned_per_promotions()
        promotions = promotions[promotions['date'] == date].groupby('promotion_id')['commission'].sum().to_dict()

        # Get the total commissions earned, the average commissions per order, and the commissions earned per promotion
        total_commissions = self.total_commissions_per_day().set_index('date').loc[date, 'commission']
        order_average = self.average_amount_of_commissions_per_order().set_index('date').loc[date, 'commission']
        commissions = {
            "promotions": promotions,
            "total": total_commissions,
            "order_average": order_average
        }

        # Create a dictionary with all the statistics for the specified date
        result = {
            "customers": int(customers),
            "total_discount_amount": float(total_discount_amount),
            "items": int(items),
            "order_total_avg": float(order_total_avg),
            "discount_rate_avg": float(discount_rate_avg),
            "commissions": commissions
        }

        return result


if __name__ == '__main__':
    table_saver = TableSaver()
    table_saver.iterate_csv_files('/data')
    statistics = Statistics()

