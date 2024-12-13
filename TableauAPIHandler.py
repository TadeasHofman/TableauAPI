import tkinter as tk
import datetime
from tkinter import filedialog
import customtkinter
import pandas as pd
from openpyxl import load_workbook
import os
import tableauserverclient as TSC
from io import StringIO
class TableauWorkbookDownloader:
    def __init__(self, server_url, token_name, token_secret):
        self.server_url = server_url
        self.personal_access_token_name = token_name
        self.personal_access_token_secret = token_secret
        self.server = None

    def connect_to_server(self):
        if not self.server:  # Connect to the server only if not already connected
            print("Connecting to Tableau Server...")
            self.server = TSC.Server(self.server_url, use_server_version=True)
            tableau_auth = TSC.PersonalAccessTokenAuth(self.personal_access_token_name, self.personal_access_token_secret)
            self.server.auth.sign_in(tableau_auth)
            print("Connected to Tableau Server.")

    def find_workbook_by_name(self, workbook_name):
        print(f"Searching for workbooks with the name '{workbook_name}'...")
        req_option = TSC.RequestOptions()
        req_option.filter.add(TSC.Filter(TSC.RequestOptions.Field.Name, TSC.RequestOptions.Operator.Equals, workbook_name))

        all_workbooks = []
        req_option.page_size = 100
        req_option.page_number = 1

        while True:
            workbooks, pagination_item = self.server.workbooks.get(req_option)
            all_workbooks.extend(workbooks)

            if not pagination_item or pagination_item.page_number * req_option.page_size >= pagination_item.total_available:
                break

            req_option.page_number += 1

        if all_workbooks:
            print(f"Found {len(all_workbooks)} workbooks with the name '{workbook_name}':")
            for wb in all_workbooks:
                print(f"Name: {wb.name}, ID: {wb.id}")
        else:
            print(f"No workbooks found with the name '{workbook_name}'.")

        return all_workbooks
    def find_workbook_by_name_and_id(self, workbook_name, workbook_id):
        print(f"Searching for workbook '{workbook_name}' with ID '{workbook_id}'...")
        req_option = TSC.RequestOptions()
        req_option.filter.add(TSC.Filter(TSC.RequestOptions.Field.Name, TSC.RequestOptions.Operator.Equals, workbook_name))

        all_workbooks = []
        req_option.page_size = 100
        req_option.page_number = 1

        while True:
            workbooks, pagination_item = self.server.workbooks.get(req_option)
            all_workbooks.extend(workbooks)

            if not pagination_item or pagination_item.page_number * req_option.page_size >= pagination_item.total_available:
                break

            req_option.page_number += 1

        matching_workbooks = [wb for wb in all_workbooks if wb.id == workbook_id]
        print(f"Found {len(matching_workbooks)} matching workbooks.")

        return matching_workbooks

    def download_view_as_dataframe(self, workbook_name, workbook_id, view_name, filters=None, batch_size=50):
        print(f"Starting download for view '{view_name}' in workbook '{workbook_name}'...")
        self.connect_to_server()

        matching_workbooks = self.find_workbook_by_name_and_id(workbook_name, workbook_id)
        if not matching_workbooks:
            print(f"Workbook '{workbook_name}' with ID '{workbook_id}' not found.")
            return None

        workbook = matching_workbooks[0]

        print(f"Populating views for workbook '{workbook_name}'...")
        self.server.workbooks.populate_views(workbook)
        view = next((v for v in workbook.views if v.name == view_name), None)
        if not view:
            print(f"View '{view_name}' not found in workbook '{workbook_name}'.")
            return None

        all_batches = []  # List to store all data frames
        tableau_columns_order = None  # Placeholder for the correct column order

        # Ensure unique filter values and split into batches if necessary
        if filters and isinstance(filters, dict):
            print(f"Applying filters to view '{view_name}'...")
            for key, values in filters.items():
                unique_values = list(set(values))
                print(f"Found {len(unique_values)} unique filter values for key '{key}'.")

                for i in range(0, len(unique_values), batch_size):
                    batch_values = unique_values[i:i + batch_size]
                    csv_req_options = TSC.CSVRequestOptions()
                    csv_req_options.vf(key, ",".join(map(str, batch_values)))
                    self.server.views.populate_csv(view, csv_req_options)

                    try:
                        raw_csv_data = b''.join(view.csv).decode('utf-8')
                        batch_data = pd.read_csv(StringIO(raw_csv_data), sep=',', low_memory=False)

                        if tableau_columns_order is None:
                            tableau_columns_order = list(batch_data.columns)

                        batch_data = batch_data[tableau_columns_order]
                        all_batches.append(batch_data)
                    except Exception as e:
                        print(f"Error reading CSV data: {e}")
                        return None
        else:
            print(f"No filters applied. Downloading view '{view_name}' without filters...")
            csv_req_options = TSC.CSVRequestOptions()
            self.server.views.populate_csv(view, csv_req_options)

            try:
                raw_csv_data = b''.join(view.csv).decode('utf-8')
                batch_data = pd.read_csv(StringIO(raw_csv_data), sep=',', low_memory=False)
                tableau_columns_order = list(batch_data.columns)
                batch_data = batch_data[tableau_columns_order]
                all_batches.append(batch_data)
            except Exception as e:
                print(f"Error reading CSV data: {e}")
                return None

        print(f"Combining all downloaded batches into a single DataFrame...")
        combined_data = pd.concat(all_batches, ignore_index=True)
        return combined_data
    def save_to_excel(self, df, file_name):
        """
        Saves the DataFrame to an Excel file.

        :param df: pandas DataFrame to save
        :param file_name: Name of the Excel file to save (with .xlsx extension)
        """
        try:
            df.to_excel(file_name, index=False)
            print(f"DataFrame saved to Excel file: {file_name}")
        except Exception as e:
            print(f"Failed to save DataFrame to Excel. Error: {e}")

    def save_to_csv(self, df, file_name):
        """
        Saves the DataFrame to a CSV file.

        :param df: pandas DataFrame to save
        :param file_name: Name of the CSV file to save (with .csv extension)
        """
        try:
            df.to_csv(file_name, index=False)
            print(f"DataFrame saved to CSV file: {file_name}")
        except Exception as e:
            print(f"Failed to save DataFrame to CSV. Error: {e}")

    # Example usage:

"USAGE EXAMPLE"
server_url = ""
token_name = ""
token_secret = ""
workbook_name = ""
view_name = ""
workbook_id = ""
workbookHandler = TableauWorkbookDownloader(server_url=server_url, token_name=token_name, token_secret=token_secret)
workbookHandler.connect_to_server()
workbookHandler.find_workbook_by_name(workbook_name)
dataframe = workbookHandler.download_view_as_dataframe(workbook_name, workbook_id, view_name)
workbookHandler.save_to_csv(dataframe, "output.csv")

