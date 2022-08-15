#!/usr/bin/env python
"""
Download files from an URL and save them to a local destination
"""
import argparse
import logging
import wandb
import requests
import tempfile


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="download_file")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    # artifact_local_path = run.use_artifact(args.input_artifact).file()

    logger.info(f"Downloading {args.sample} ...")
    with tempfile.NamedTemporaryFile(mode='wb+') as fp:
   
        # Download the file streaming and write to open temp file
        with requests.get(args.sample, stream=True) as r:
            #for chunk in r.iter_content(chunk_size=8192):
            fp.write()chunk)

            # Make sure the file has been written to disk before uploading
            # to W&B
            fp.flush()

            logger.info("Creating run")
            with wandb.init(job_type="download_file") as run:

            logger.info(f"Uploading {args.artifact_name} to Weights & Biases")
            artifact = wandb.Artifact(
                name=args.artifact_name,
                type=args.artifact_type,
                description=args.artifact_description,
                metadata={'original_url': args.sample}
            )
            artifact.add_file(fp.name, name=basename)

            logger.info("Logging artifact")
            run.log_artifact(artifact)

            artifact.wait()
   # artifact_local_path = run.use_artifact(args.input_artifact).file()
    
    
    logger.info(f"Uploading {args.artifact_name} to Weights & Biases")
    log_artifact(
        args.artifact_name,
        args.artifact_type,
        args.artifact_description,
        os.path.join("data", args.sample),
        run,
    )


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="download a URL to a local destination")


    parser.add_argument(
        "--sample", 
        type = str,                               ## INSERT TYPE HERE: str, float or int,
        help = "Name of the sample to download"   ## INSERT DESCRIPTION HERE,
        required = True
    )

    parser.add_argument(
        "--artifact_name", 
        type = str,                               ## INSERT TYPE HERE: str, float or int,
        help = "Name for the output artifact"     ## INSERT DESCRIPTION HERE,
        required = True
    )

    parser.add_argument(
        "--artifact_type", 
        type = str,                               ## INSERT TYPE HERE: str, float or int,
        help = "Output artifact type"             ## INSERT DESCRIPTION HERE,
        required = True
    )

    parser.add_argument(
        "--artifact_description", 
        type = str,                               ## INSERT TYPE HERE: str, float or int,
        help = "A brief description of this artifact"   ## INSERT DESCRIPTION HERE,
        required = True
    )


    args = parser.parse_args()

    go(args)
