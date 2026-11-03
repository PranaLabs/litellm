from typing import Final

from litellm.integrations.gcs_archive import create_gcs_archive_logger

logger: Final = create_gcs_archive_logger()
