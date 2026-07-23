from tenki_sandbox._pb import pb


def test_legacy_project_request_wire_decodes_after_workspace_field_expansion():
    project_id = "019f69c7-96c9-7971-83d8-896b9d035b36"
    encoded_project_id = project_id.encode()
    wire = bytes((0x0A, len(encoded_project_id))) + encoded_project_id

    request = pb.ListProjectSandboxesRequest.FromString(wire)

    assert request.project_id == project_id
    assert not request.HasField("workspace_id")
