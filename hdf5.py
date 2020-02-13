import os
import h5py
import numpy as np
import concurrent.futures
from contextlib import contextmanager
from google.oauth2 import service_account
from google.cloud import storage


@contextmanager
def yield_file(HDF_PATH):
    with h5py.File(HDF_PATH, 'r') as file:
        yield file


def process_groups(group, path, timestamp):
    with yield_file(path) as file:
        dataset = file.get('series')
        #timestamp = file.attrs.get('start_timestamp', 0)
        dataframe = dataset.get(group)
        attrs = dataframe.attrs
        members = list(dataframe.keys())

        if "submasks" in members:
            members.remove("submasks")
            data = [dataframe.get(member)[:] for member in members]
            data.append([submask for submask in dataframe.get("submasks")])
            members.insert(len(members)+1, "submasks")
        else:
            data = [dataframe.get(member)[:] for member in members]

        sup_offset = attrs.get('sup_offset', 0)
        frequency = attrs.get('frequency', 0)
        timestamp_arr = []
        timestamp_arr.append(timestamp + sup_offset)

        for i in range(len(data[0])-1):
            timestamp = timestamp + sup_offset + frequency
            timestamp_arr.append(timestamp)

        material = np.empty(len(data[0]), dtype="S256")
        material.fill((dataframe.name).split("/")[2])

        members.insert(0, 'timestamp')
        members.insert(0, 'material')

        data.insert(0, timestamp_arr)
        data.insert(0, material)

        data_np = np.array(data)
        data_np = np.column_stack(data_np)

        name_file = f"{((dataframe.name).split('/')[2]).replace(' ', '_')}_{timestamp}.csv"
        print(f"Procesando archivo: {name_file}")
        np.savetxt(f"{os.getcwd()}/files/{name_file}", data_np, delimiter=";", fmt="%s",
                    header=";".join([member for member in members]))
        print("Archivo procesado correctamente")

def main():
    print("Iniciando proceso de HDF5")
    credentials = service_account.Credentials.from_service_account_file(
            f"{os.getcwd()}/credentials.json")

    client = storage.Client(credentials=credentials)
    bucket = client.get_bucket('')
    blob = bucket.blob('')

    path = "files"
    if not os.path.exists(path):
        os.makedirs(path, 777)

    print("Descargando Archivo HDF5")
    destination_uri = f"{path}/{blob.name}"
    blob.download_to_filename(destination_uri)

    print("ProcessPoolExecutor Step")

    HDF_PATH = destination_uri

    groups = None
    timestamp = None
    with h5py.File(HDF_PATH, 'r') as hdf5_file:
        groups = list(hdf5_file.get('series').keys())
        timestamp = hdf5_file.attrs.get('start_timestamp', 0)

    with concurrent.futures.ProcessPoolExecutor(max_workers=10) as executor:
        for group, res in ((group, executor.submit(process_groups, group,
                HDF_PATH, timestamp)) for group in groups):
            print(res.result())


if __name__ == "__main__":
    main()
