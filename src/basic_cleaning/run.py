#!/usr/bin/env python
"""
Performs basic cleaning on the data and save the results in Weights & Biases
"""
import argparse
import logging
import wandb
import pandas as pd
import os


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    # artifact_local_path = run.use_artifact(args.input_artifact).file()

    logger.info("Downloading artifact")
    artifact_local_path = run.use_artifact(args.input_artifact).file()

    df = pd.read_csv(artifact_local_path)

    # Drop the duplicates
    logger.info("Dropping duplicates")
    df = df.drop_duplicates().reset_index(drop=True)

    # Drop outliers
    logger.info("Dropping outliers")
    min_price = float(args.min_price)   #10
    max_price = float(args.max_price)   #350
    idx = df['price'].between(min_price, max_price)
    df = df[idx].copy()
    
    # Convert last_review to datetime
    logger.info("Convert last_review to datetime")
    df['last_review'] = pd.to_datetime(df['last_review'])

    # Missing values
    # Note how we did not impute missing values.
    # We will do that in the inference pipeline,
    # so we will be able to handle missing values also in production
    logger.info("Drop rows with missing values :-/ Only here!")
    df.dropna(inplace=True)

    filename = "clean_sample.csv"
    df.to_csv(filename, index=False)
    
    artifact = wandb.Artifact(
        name=args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )
    artifact.add_file(filename)

    logger.info("Logging artifact")
    run.log_artifact(artifact)

    os.remove(filename)

    #logger.info("Finish W&B run")
    #run.finish()


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="This steps cleans the data")

    parser.add_argument(
        "--input_artifact", 
        type=str,                                        ## INSERT TYPE HERE: str, float or int,
        help="Fully qualified name for the artifact",   ## INSERT DESCRIPTION HERE,
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,                                                ## INSERT TYPE HERE: str, float or int,
        help="Name for the W&B artifact that will be created",   ## INSERT DESCRIPTION HERE,
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,                                    ## INSERT TYPE HERE: str, float or int,
        help="Type of the artifact to create",       ## INSERT DESCRIPTION HERE,
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,                                    ## INSERT TYPE HERE: str, float or int,
        help="Description for the artifact ",        ## INSERT DESCRIPTION HERE,
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,                              ## INSERT TYPE HERE: str, float or int,
        help="The minimum price to consider",    ## INSERT DESCRIPTION HERE,
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,                              ## INSERT TYPE HERE: str, float or int,
        help="The minimum price to consider",    ## INSERT DESCRIPTION HERE,
        required=True
    )

    args = parser.parse_args()

    go(args)
