.PHONY: run_simulator

teams := "7f4b0017-4d2e-4aa5-9e1b-9bb0f1a84eab" \
         "9b1c74c2-7e42-4e82-bf3d-305b7f23d789" \
         "8fc1f93e-2a3f-4a43-9dd0-ff73c50b6f3d" \
         "e6b8a7b4-0d5d-47d8-99fd-c8e2d61a6f30" \
         "cd57c4f7-11d2-4fcb-835f-8fa4ef2f269f" \
         "d5ebcb1e-8e88-4761-ae26-d0d6b5697e3c" \
         "58c482fe-c4f2-4b6b-bfe4-572fe7b252d0" \
         "5bc3731a-1084-4e18-a18d-503e258b6667" \
         "b18b446f-5754-4e92-8f60-15b8f0d4c6b1" \
         "ee61aafd-9f9b-4e99-89c0-7051f6e676d3" \
         "f2d8c32e-4d29-404a-ae06-010d9a2bcb8b" \
         "cfdad3a0-b4d4-456d-8c9f-4564c10303b8" \
         "e4e18a77-0d08-47e7-bfb6-2a5f8d4e5d74" \
         "4f961d25-6e3e-4e5b-9e69-8f9c2f50ac1f" \
         "1760f2c0-7ed3-4727-baf4-676a66d9f34f" \
         "19f8e35d-1225-452d-926a-1c5a2bbf29a8" \
         "86c7ab63-973d-4b61-8d24-09729f9b9754" \
         "c3ee428b-77cd-4c3d-9d57-1ea3f8ea1778" \
         "29bda569-5b0c-4ce7-af9b-47a1f918c9a7" \
         "db7e1b20-efcc-4f71-a98e-cb0b3c3e086b" \
         "23c0b15f-bfa5-4b6a-aae3-53f6c54ce9b4" \
         "fe4784e0-978c-4f39-a84e-bb5b67a3e292" \
         "e3b5f2e5-13b3-491e-b9e1-4e35b66efab4" \
         "cf69f647-d4e1-4957-b8eb-35f0e6146d77" \
         "4c5b213b-68a6-4bb1-bb7d-92f4817e24e7" \
         "3b193206-93c5-4624-9f4b-c06530c0a2e5" \
         "1e9de6e2-1e27-4e94-8ea1-bbfc264fa586" \
         "d06736c1-019e-49f7-bb2a-1b5d34f605fa" \
         "ccfe157a-3888-4f30-a0f7-91b2e80174f0" \
         "5b5a3a9a-4f7e-4827-9c3f-4b8b1dfc0ec1" \
         "6f4d71b5-1556-4266-bb14-41903a543dae" \
         "9ef5a849-b1ef-4c60-80cf-2c44e7bc54b6" \
         "2032f07b-7e4c-46a4-8b53-e2e23633b3b1" \
         "755c0b4a-6813-4f5f-af0e-5abaf0ccdb98" \
         "96de479c-d3b6-47ac-9cf2-b60f5e3294e1" \
         "22d2002c-9ed3-4ce4-9f68-d4dd3101f2a4" \
         "b51a4251-cdd4-4e15-9a30-597e397e91c6" \
         "18731f1d-6eb5-4270-bf4c-9f23812e19f0" \
         "0f73f17e-2738-4e20-bd36-5c2e0211a4b5"

run_simulator:
	python simulator_runner.py --worker-id guinea-pig --num-envs 36 --batch-size 1 --teams $(teams)

