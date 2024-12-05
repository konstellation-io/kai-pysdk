from vyper import v

from sdk.metadata.metadata import Metadata


def test_ok():
    v.set("metadata.product_id", "test_product_id")
    v.set("metadata.version_tag", "test_version_id")
    v.set("metadata.workflow_name", "test_workflow_id")
    v.set("metadata.workflow_type", "test_workflow_type")
    v.set("metadata.process_name", "test_process_id")
    v.set("metadata.process_type", "test_process_type")
    v.set("nats.object_store", "test_object_store")
    v.set("centralized_configuration.global.bucket", "test_global")
    v.set("centralized_configuration.product.bucket", "test_product")
    v.set("centralized_configuration.workflow.bucket", "test_workflow")
    v.set("centralized_configuration.process.bucket", "test_process")

    metadata = Metadata()

    assert metadata.get_product() == "test_product_id"
    assert metadata.get_version() == "test_version_id"
    assert metadata.get_workflow() == "test_workflow_id"
    assert metadata.get_workflow_type() == "test_workflow_type"
    assert metadata.get_process() == "test_process_id"
    assert metadata.get_process_type() == "test_process_type"
    assert metadata.get_ephemeral_storage_name() == "test_object_store"
    assert metadata.get_global_centralized_configuration_name() == "test_global"
    assert metadata.get_product_centralized_configuration_name() == "test_product"
    assert metadata.get_workflow_centralized_configuration_name() == "test_workflow"
    assert metadata.get_process_centralized_configuration_name() == "test_process"
