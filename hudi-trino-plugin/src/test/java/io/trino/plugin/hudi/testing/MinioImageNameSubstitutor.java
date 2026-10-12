/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.hudi.testing;

import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.ImageNameSubstitutor;

public class MinioImageNameSubstitutor
        extends ImageNameSubstitutor
{
    // Use the pinned MinIO image from Trino's current test infrastructure.
    private static final DockerImageName MINIO_IMAGE = DockerImageName.parse(
            "cgr.dev/chainguard/minio@sha256:6a1d0b45c8669726bba580ced0bfa4cb9fdeed1ed636dfabd81d1577beb6937b");

    @Override
    public DockerImageName apply(DockerImageName original)
    {
        if (original.getRepository().equals("minio/minio")) {
            return MINIO_IMAGE.asCompatibleSubstituteFor(original);
        }
        return original;
    }

    @Override
    protected String getDescription()
    {
        return "Use the available MinIO test image";
    }
}
