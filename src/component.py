'''
Template Component main class.

'''
import csv
import logging
from datetime import datetime

from keboola.component.base import ComponentBase
from keboola.component import UserException

# configuration variables
KEY_PRINT_ROWS = 'print_rows'

# list of mandatory parameters => if some is missing,
# component will fail with readable message on initialization.
REQUIRED_PARAMETERS = [KEY_PRINT_ROWS]
REQUIRED_IMAGE_PARS = []


class Component(ComponentBase):
    """
        Extends base class for general Python components. Initializes the CommonInterface
        and performs configuration validation.

        For easier debugging the data folder is picked up by default from `../data` path,
        relative to working directory.

        If `debug` parameter is present in the `config.json`, the default logger is set to verbose DEBUG mode.
    """

    def __init__(self):
        super().__init__(required_parameters=REQUIRED_PARAMETERS,
                         required_image_parameters=REQUIRED_IMAGE_PARS)

    def run(self):
        '''
        Main execution code
        '''

        params = self.configuration.parameters

        input_table = self.get_input_tables_definitions()
        input_table_path = input_table[0].full_path
        logging.info(input_table_path)

        # get last state data/in/state.json from previous run
        previous_state = self.get_state_file()
        logging.info('last_update: ' + previous_state.get('last_update'))

        # Create output table (Tabledefinition - just metadata)
        table = self.create_out_table_definition('output.csv', incremental=True, primary_key=['row_number'])

        # get file path of the table (data/out/tables/Features.csv)
        out_table_path = table.full_path
        logging.info(out_table_path)

        # DO whatever and save into out_table_path
        with open(input_table_path, "r") as input_file, open(
            out_table_path, mode="wt", encoding="utf-8", newline=""
        ) as out_file:
            reader = csv.DictReader(input_file)
            new_columns = reader.fieldnames
            # append row number col
            new_columns.append('row_number')
            writer = csv.DictWriter(out_file, fieldnames=new_columns, lineterminator='\n', delimiter=',')
            writer.writeheader()
            for index, l in enumerate(reader):
                # add row number
                l['row_number'] = index
                # print line
                if params.get(KEY_PRINT_ROWS):
                    logging.info(f'Printing line {index}: {l}')
                writer.writerow(l)

        # Save table manifest (output.csv.manifest) from the tabledefinition
        self.write_manifest(table)

        # Write new state - will be available next run
        self.write_state_file({"last_update": datetime.now().isoformat()})

        # ####### EXAMPLE TO REMOVE END


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        comp.run()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
