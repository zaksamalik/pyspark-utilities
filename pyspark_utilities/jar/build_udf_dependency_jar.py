import os


def build_udf_dependency_jar(jar_output_location):
    """Build dependency JAR and save to specified output location.

    Args:
        jar_output_location (str): Location to copy JAR file.

    Returns:
    """
    this_file_dir = os.path.dirname(os.path.abspath(__file__))
    print(f"Building JAR and saving to `{jar_output_location}`.")
    os.system(f"rm -rf {this_file_dir}/target")
    os.system(f"cd {this_file_dir} && mvn package")

    if not os.path.exists(jar_output_location):
        os.mkdir(jar_output_location)

    os.system(f"cp {this_file_dir}/target/pyspark-utilities-LATEST-jar-with-dependencies.jar {jar_output_location}")
