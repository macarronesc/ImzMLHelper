from pyimzml.ImzMLParser import ImzMLParser
import numpy as np


class ImzMLHelper:
    """
    The ImzMLHelper class provides some functionalities to help reading a imzML file stored
    in the cloud using the pyimzML library. We take advantage of range downloads to avoid
    downloading the whole file.

    Args:
        bucket (str): Storage bucket where the file is stored
        key_imz (str): Imz file key inside the bucket
        key_ibd (str): Ibd file key inside the bucket
        filename_imz (str): Name that will be given to the local imz file
        filename_ibd (str): Name that will be given to the local ibd file
        s3: boto3 instance with access to the Object Storage
    """

    def __init__(self, bucket: str, key_imz: str, key_ibd: str, filename_imz: str, filename_ibd: str, s3):
        self.bucket = bucket

        self.key_imz = key_imz
        self.key_ibd = key_ibd

        self.filename_imz = filename_imz
        self.filename_ibd = filename_ibd

        self.ibm_cos = s3

    def load_filename_imz(self):
        """Download the imz file and store it in a local file. This function should be one of the first
        to be called.
        """
        self.ibm_cos.download_file(self.bucket, self.key_imz, self.filename_imz)

    def load_filename_ibd(self):
        """Download the ibd file and store it in a local file. This function should be one of the first
        to be called.
        """
        self.ibm_cos.download_file(self.bucket, self.key_ibd, self.filename_ibd)

    def split_imz(self):
        """Split the imz file into small parts, the number of parts is defined by the number of coordinates.
        The imz and ibd file needs to be loaded first.

        Returns:
            List[tuple[ndarray, ndarray]]: MzArray and IntensityArray
        """
        f = open(self.filename_imz, "r+b")
        f_ibd = open(self.filename_ibd, "r+b")
        parser = ImzMLParser(f, ibd_file=f_ibd)
        data = []

        for i in range(len(parser.coordinates)):
            data.append(parser.getspectrum(i))

        f.close()
        f_ibd.close()
        return data

    def get_all_coordinates(self):
        """Get all coordinates in our data. The imz file needs to be loaded first.

        Returns:
            List: Coordinates
        """
        f = open(self.filename_imz, "r+b")
        parser = ImzMLParser(f, ibd_file=None)
        coordinates = parser.coordinates
        f.close()
        return coordinates

    def get_metadata(self):
        """Get all metadata of our data. The imz file needs to be loaded first.

        Returns:
            Dict[str, Union[dict, str]]: All metadata info.
        """
        print(self.filename_imz)
        f = open(self.filename_imz, "r+b")
        parser = ImzMLParser(f, ibd_file=None)

        info = {"imzML filename": self.filename_imz, "ibd filename": self.filename_ibd,
                "Polarity": parser.polarity, "Int Group Id": parser.intGroupId,
                "Basic imzML metadata for reading spectra": parser.imzmldict}

        f.close()
        return info

    def get_intensity_info(self):
        """Get the intensity info of all points in our data. The imz file needs to be loaded first.

        Returns:
            Dict[str, Union[dict, str]]: All intensity info.
        """
        precision_aux_intensity = 0
        f = open(self.filename_imz, "r+b")
        parser = ImzMLParser(f, ibd_file=None)

        # Get precision in understandable words.
        for key, value in parser.precisionDict.items():
            if parser.intensityPrecision == value:
                precision_aux_intensity = key

        info = {"Intensity Offsets": parser.intensityOffsets, "Intensity Precision": precision_aux_intensity,
                "Intensity Lengths": parser.intensityLengths}

        f.close()
        return info

    def get_intensity_info_point(self, coordinate: tuple):
        """Get the intensity info of one specific point in our data. The imz file needs to be loaded first.

        Returns:
                Dict[str, Union[dict, str]]: All intensity info.
        """
        precision_aux_intensity_point = 0
        f = open(self.filename_imz, "r+b")
        parser = ImzMLParser(f, ibd_file=None)
        position = parser.coordinates.index(coordinate)

        # Get precision in understandable words.
        for key, value in parser.precisionDict.items():
            if parser.intensityPrecision == value:
                precision_aux_intensity_point = key

        info = {"Intensity Offsets": parser.intensityOffsets[position], "Intensity Precision":
                precision_aux_intensity_point, "Intensity Lengths": parser.intensityLengths[position]}

        f.close()
        return info

    def get_mz_info(self):
        """Get the mz info of all points in our data. The imz file needs to be loaded first.

        Returns:
            Dict[str, Union[dict, str]]: All mz info.
        """
        precision_aux_mz = 0
        f = open(self.filename_imz, "r+b")
        parser = ImzMLParser(f, ibd_file=None)

        # Get precision in understandable words.
        for key, value in parser.precisionDict.items():
            if parser.mzPrecision == value:
                precision_aux_mz = key

        info = {"mz Offsets": parser.mzOffsets, "mz Group Id": parser.mzGroupId,
                "mz Precision": precision_aux_mz, "mz Lengths": parser.mzLengths}
        f.close()
        return info

    def get_mz_info_point(self, coordinate: tuple):
        """Get the mz info of one specific point in our data. The imz file needs to be loaded first.

        Returns:
            Dict[str, Union[dict, str]]: All mz info.
        """
        precision_aux_mz_point = 0
        f = open(self.filename_imz, "r+b")
        parser = ImzMLParser(f, ibd_file=None)
        position = parser.coordinates.index(coordinate)

        # Get precision in understandable words.
        for key, value in parser.precisionDict.items():
            if parser.mzPrecision == value:
                precision_aux_mz_point = key

        info = {"mz Offsets": parser.mzOffsets[position], "mz Group Id": parser.mzGroupId,
                "mz Precision": precision_aux_mz_point, "mz Lengths": parser.mzLengths[position]}
        f.close()
        return info

    def get_data_point_local(self, coordinate: tuple):
        """Get data of one specific point in our data, this method needs the entire downloaded ibd file.
        The imz and ibd file needs to be loaded first.

        Returns:
            Tuple[ndarray, ndarray]: Data of the specific point.
        """
        f = open(self.filename_imz, "r+b")
        f_ibd = open(self.filename_ibd, "r+b")
        parser = ImzMLParser(f, ibd_file=f_ibd)

        position = parser.coordinates.index(coordinate)

        info = parser.getspectrum(position)
        f.close()
        f_ibd.close()
        return info

    def get_data_point_cloud(self, coordinate: tuple):
        """Get all data in one specific point in our data, this method not needs the entire downloaded ibd file,
        it uses ranges to download a specific part.
        The imz file needs to be loaded first.

         Args:
            coordinate (tuple): Coordinates of our point.

        Returns:
            Mz array (list): Array with our mz info.
            Intensity array (list): Array with our intensity info.
        """
        return self.get_mz_array_point(coordinate), self.get_intensity_array_point(coordinate)

    def get_mz_array_point(self, coordinate: tuple):
        """Get mz array in one specific point in our data, this method not needs the entire downloaded ibd file,
        it uses ranges to download a specific part.
        The imz file needs to be loaded first.

        Returns:
            List: Mz array of our point.
        """
        f = open(self.filename_imz, "r+b")
        parser = ImzMLParser(f, ibd_file=None)
        position = parser.coordinates.index(coordinate)

        mzOffset = parser.mzOffsets[position]
        mzSize = (parser.sizeDict.get(parser.mzPrecision) * parser.mzLengths[position]) - 1

        # Download our specific data.
        header_request = self.ibm_cos.get_object(Bucket=self.bucket, Key=self.key_ibd,
                                                 Range='bytes={}-{}'.format(mzOffset,
                                                                            mzOffset + mzSize))
        mzArray = []
        for i in header_request['Body']:
            mzArray = np.append(mzArray, np.frombuffer(i, dtype=parser.intensityPrecision))

        f.close()
        return mzArray

    def get_intensity_array_point(self, coordinate: tuple):
        """Get intensity array in one specific point in our data, this method not needs the entire downloaded ibd file,
        it uses ranges to download a specific part.
        The imz file needs to be loaded first.

        Returns:
            List: Intensity array of our point.
        """
        f = open(self.filename_imz, "r+b")
        parser = ImzMLParser(f, ibd_file=None)
        position = parser.coordinates.index(coordinate)

        intensityOffset = parser.intensityOffsets[position]
        intensitySize = (parser.sizeDict.get(parser.intensityPrecision) * parser.intensityLengths[position]) - 1

        # Download our specific data.
        header_request = self.ibm_cos.get_object(Bucket=self.bucket, Key=self.key_ibd,
                                                 Range='bytes={}-{}'.format(intensityOffset,
                                                                            intensityOffset + intensitySize))
        intensityArray = []
        for i in header_request['Body']:
            intensityArray = np.append(intensityArray, np.frombuffer(i, dtype=parser.intensityPrecision))

        f.close()
        return intensityArray
