from scripts.datapipe import DataPipe, SqlQuery
import logging

logging.basicConfig(filename='datapipe_logs', filemode='w', format='%(asctime)s - %(message)s', level=logging.DEBUG)


def main():

    paths = {'batches': '/media/austin/Ubuntu-2/postgres_i502/raw_data/I502/Batches_0.csv'
        , 'licensees': '/media/austin/Ubuntu-2/postgres_i502/raw_data/I502/Licensees_0.csv'
        , 'inventory': '/media/austin/Ubuntu-2/postgres_i502/raw_data/I502/Inventories_0.csv'
        , 'lab': '/media/austin/Ubuntu-2/postgres_i502/raw_data/I502/LabResults.csv'}

    lic_q = '''SELECT global_id
                        , created_at
                        , external_id
                        , name
                        , type
            FROM temp
            WHERE created_at >= CAST("{}-{}-01" AS DATE)
            '''
    batch_q = '''SELECT global_id
                        , created_at
                        , mme_id
                        , user_id
                        , external_id
                        , is_parent_batch
                        , is_child_batch
                        , type
              FROM temp
              WHERE created_at >= CAST("{}-{}-01" AS DATE)
              '''
    inventory_q = '''SELECT global_id
                                 , created_at
                                 , mme_id
                                 , user_id
                                 , external_id
                                 , batch_id
                                 , lab_result_id
                                 , created_by_mme_id
                                 , strain_id
                                 , inventory_type_id
                                 , global_original_id
                          FROM inventory
                          WHERE created_at >= CAST("{}-{}-01" AS DATE)
                             AND lab_result_id NOT LIKE "None"
                      '''
    lab_q = '''SELECT global_id
                        , created_at
                        , mme_id
                        , user_id
                        , external_id
                        , inventory_id
                        , updated_at
                        , status
                        , testing_status
                        , batch_id
                        , for_mme_id
                        , lab_user_id
                        , type
                        , cannabinoid_status
                        , cannabinoid_editor
                        , cannabinoid_d9_thc_percent
                        , cannabinoid_cbd_percent
                        , cannabinoid_cbda_percent
                FROM lab_tests
                WHERE created_at >= CAST("{}-{}-01" AS DATE)
                '''

    queries = {'batches': SqlQuery(batch_q, 2020, 1)
        , 'licensees': SqlQuery(lic_q, 1899, 1)
        , 'inventory': SqlQuery(inventory_q, 2020, 1)
        , 'lab': SqlQuery(lab_q, 2020, 1)}

    finished = ['batches', 'licensees']

    for key, value in paths.items():
        if key in finished:
            continue
        path = value
        query = queries[key]
        try:
            _ = DataPipe(path, query, key)
        except:
            logging.exception('Exception occurred during DataPipe execution')
            continue


if __name__ == '__main__':
    main()
