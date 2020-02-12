import os
import h5py
import numpy as np
import concurrent.futures
from google.oauth2 import service_account
from google.cloud import storage


def process_groups(group, dataset, timestamp):
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
        timestamp_arr.append(timestamp + sup_offset + (
                                frequency))

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
            f"{os.getcwd()}/bc-te-dlake-dev-s7b3-451c22a74cc1.json")

    client = storage.Client(credentials=credentials)
    bucket = client.get_bucket('us-east-1-dataflow-econtreras')
    blob = bucket.blob('input_hdf5/FCB_1_5B-DCV_20180827062547_42_TDQAR___media__root__DCVDMU1__EOFLOCATQAR.dat-QAR.DAT.001-START_AND_STOP.hdf5')

    path = "files"
    if not os.path.exists(path):
        os.makedirs(path, 777)

    print("Descargando Archivo HDF5")
    destination_uri = f"{path}/{blob.name}"
    blob.download_to_filename(destination_uri)

    print("ProcessPoolExecutor Step")
    with h5py.File(destination_uri, 'r') as file:
        timestamp = file.attrs.get('start_timestamp', 0)
        dataset = file.get('series')

        executor = concurrent.futures.ProcessPoolExecutor(4)
        futures = [executor.submit(process_groups, group, dataset, timestamp) for group in dataset]
        concurrent.futures.wait(futures)


if __name__ == "__main__":
    main()


