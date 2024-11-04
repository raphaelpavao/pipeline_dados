import json
import csv

class Data:
    def __init__(self, path, file_type):
        self.path = path
        self.file_type = file_type
        self.data = self.read_data()
        self.column_names = self.get_columns()
        self.size = self.data_size()

    def read_json(self):
        json_data = []
        with open(self.path, 'r') as file:
            json_data = json.load(file)
        return json_data

    def read_csv(self):
        csv_data = []
        with open(self.path, 'r') as file:
            reader = csv.DictReader(file, delimiter=',')
            for row in reader:
                csv_data.append(row)
        return csv_data

    def read_data(self):
        data = []
        if self.file_type == 'csv':
            data = self.read_csv()
        elif self.file_type == 'json':
            data = self.read_json()
        elif self.file_type == 'list':
            data = self.path
            self.path = 'in-memory list'
        return data

    def get_columns(self):
        return list(self.data[0].keys())

    def rename_columns(self, key_mapping):
        new_data = []
        for old_dict in self.data:
            temp_dict = {}
            for old_key, value in old_dict.items():
                temp_dict[key_mapping[old_key]] = value
            new_data.append(temp_dict)
        self.data = new_data
        self.column_names = self.get_columns()

    def data_size(self):
        return len(self.data)

    @staticmethod
    def join_data(dataA, dataB):
        combined_list = []
        combined_list.extend(dataA.data)
        combined_list.extend(dataB.data)
        return Data(combined_list, 'list')

    def data_to_table(self):
        combined_table = [self.column_names]
        
        for row in self.data:
            line = []
            for column in self.column_names:
                line.append(row.get(column, 'Unavailable'))
            combined_table.append(line)
        return combined_table
    
    def save_data(self, path): 
        combined_table = self.data_to_table()
        with open(path, 'w') as file: 
            writer = csv.writer(file)
            writer.writerows(combined_table)

