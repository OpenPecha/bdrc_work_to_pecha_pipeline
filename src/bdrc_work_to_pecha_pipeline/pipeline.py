from bdrc_work_to_pecha_pipeline.logger import get_logger
from bdrc_work_to_pecha_pipeline.pecha_upload import get_work_batches, run_pipeline

# Get a logger for this module
logger = get_logger(__name__)


def main(work_id: str):
    logger.info(f"Starting pipeline for work ID: {work_id}")
    # Get all batches for this work ID
    batches = get_work_batches(work_id)

    if not batches:
        logger.warning(f"No OCR data found for work ID: {work_id}")
    else:
        logger.info(f"Found {len(batches)} batch(es) for work ID: {work_id}")

        for batch_info in batches:
            work_id, ocr_engine, batch_number = batch_info
            logger.info(
                f"\n🔍 Processing: work_id={work_id}, ocr_engine={ocr_engine}, batch={batch_number}"
            )

            try:
                run_pipeline(
                    work_id=work_id, batch_number=batch_number, ocr_engine=ocr_engine
                )
                logger.info(
                    f"✅ Successfully processed {work_id}/{ocr_engine}/{batch_number}"
                )
            except Exception as e:
                logger.error(
                    f"❌ Error processing {work_id}/{ocr_engine}/{batch_number}: {e}"
                )
                # Continue with the next batch even if one fails
                continue


if __name__ == "__main__":
    work_ids = ["W24767"]
    for work_id in work_ids:
        main(work_id)
