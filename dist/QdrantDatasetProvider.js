import { QdrantClient } from '@qdrant/js-client-rest';
import dotenv from 'dotenv';
import { v5 as uuidv5 } from 'uuid';
dotenv.config();
export class QdrantDatasetProvider {
    constructor(qdrantUrl, vectorSize = 1536, apiKey) {
        this.qdrant = new QdrantClient({
            url: qdrantUrl,
            apiKey, // Include API Key if provided
        });
        this.vectorSize = vectorSize;
    }
    /**
     * Factory method to create an instance of QdrantDatasetProvider
     */
    static async create() {
        const qdrantUrl = process.env.QDRANT_URL;
        if (!qdrantUrl) {
            throw new Error('QDRANT_URL is not defined in the environment variables.');
        }
        const vectorSize = Number(process.env.VECTOR_SIZE) || 1536;
        const apiKey = process.env.QDRANT_API_KEY; // Retrieve API Key from .env
        return new QdrantDatasetProvider(qdrantUrl, vectorSize, apiKey);
    }
    /**
     * Converts a rowId to a deterministic UUID using UUID v5.
     * @param rowId - The original rowId string.
     * @returns A UUID string.
     */
    rowIdToUUID(rowId) {
        return uuidv5(rowId, QdrantDatasetProvider.UUID_NAMESPACE);
    }
    /**
     * Constructs the collection name based on projectId and datasetId.
     * Format: <projectId>.<datasetId>
     */
    getCollectionName(projectId, datasetId) {
        return `${projectId}.${datasetId}`;
    }
    /**
     * Extracts the projectId from the collection name.
     * @param collectionName - The name of the collection in the format <projectId>.<datasetId>.
     * @returns The extracted ProjectId.
     */
    getProjectIdFromCollectionName(collectionName) {
        const parts = collectionName.split('.');
        if (parts.length < 2) {
            throw new Error(`Invalid collection name format: ${collectionName}`);
        }
        const projectId = parts[0];
        return projectId;
    }
    /**
     * Finds the collection name for a given datasetId by searching all collections.
     * @param datasetId - The ID of the dataset.
     * @returns The collection name if found.
     */
    async findCollectionName(datasetId) {
        const response = await this.qdrant.getCollections();
        const collection = response.collections.find(col => col.name.endsWith(`.${datasetId}`));
        if (!collection) {
            throw new Error(`Collection not found for DatasetId: ${datasetId}`);
        }
        return collection.name;
    }
    /**
     * Validates whether the provided vector is suitable for a search operation.
     * @param vector - The query vector to validate.
     * @returns A boolean indicating whether the vector is valid.
     */
    isValidVector(vector) {
        return (Array.isArray(vector) &&
            vector.length === this.vectorSize &&
            vector.every(num => typeof num === 'number' && !isNaN(num) && isFinite(num)));
    }
    /**
     * Performs a KNN search on the specified collection.
     * @param collectionName - The name of the collection to search.
     * @param vector - The query vector for similarity.
     * @param k - The number of nearest neighbors to retrieve.
     * @returns An array of DatasetRows matching the search criteria.
     */
    async performSearch(collectionName, vector, k) {
        try {
            console.log(`[performSearch] Searching collection '${collectionName}' with vector length ${vector.length}`);
            console.log(`[performSearch] Searching with vector: [${vector.slice(0, 5).join(', ')}...]`);
            const response = await this.qdrant.search(collectionName, {
                vector: vector, // Assign directly as number[]
                limit: k,
                with_payload: true,
                with_vector: true,
            });
            console.log(`[performSearch] Received response: ${JSON.stringify(response).substring(0, 500)}...`);
            const searchResult = response;
            const processedResults = searchResult.map((res) => {
                const rowId = res.payload?.rowId;
                if (typeof rowId !== 'string') {
                    throw new Error(`Invalid rowId type: expected string, got ${typeof rowId}`);
                }
                return {
                    id: rowId, // Use the original rowId as string
                    data: Array.isArray(res.payload?.data) ? res.payload.data.map(String) : [String(res.payload?.data)],
                    embedding: Array.isArray(res.vector) ? res.vector : [],
                    // 'distance' is optional and not part of DatasetRow, but included if needed elsewhere
                };
            });
            console.log(`[performSearch] Processed ${processedResults.length} results`);
            return processedResults;
        }
        catch (error) {
            console.error('[performSearch] Error during KNN search:', error.response?.data || error.message);
            throw error;
        }
    }
    /**
     * Retrieves points from the specified collection using the scroll method.
     * @param collectionName - The name of the collection to retrieve points from.
     * @param batchSize - The number of points to retrieve per scroll.
     * @returns An array of DatasetRows retrieved from the collection.
     */
    async performScroll(collectionName, batchSize) {
        try {
            console.log(`[performScroll] Scrolling collection '${collectionName}' with batch size ${batchSize}`);
            const scrollResult = await this.qdrant.scroll(collectionName, {
                limit: batchSize,
                filter: {}, // Define filters if needed
            });
            console.log(`[performScroll] Retrieved ${scrollResult.points.length} points`);
            const processedResults = scrollResult.points.map((point) => {
                const rowId = point.payload?.rowId;
                if (typeof rowId !== 'string') {
                    throw new Error(`Invalid rowId type: expected string, got ${typeof rowId}`);
                }
                return {
                    id: rowId,
                    data: Array.isArray(point.payload?.data) ? point.payload.data.map(String) : [String(point.payload?.data)],
                    embedding: Array.isArray(point.vector) ? point.vector : [],
                };
            });
            console.log(`[performScroll] Processed ${processedResults.length} results`);
            return processedResults;
        }
        catch (error) {
            console.error('[performScroll] Error during scroll retrieval:', error.response?.data || error.message);
            throw error;
        }
    }
    /**
     * Retrieves a dataset row by searching for a specific payload field value.
     * @param id - The DatasetId.
     * @param rowIdValue - The rowId value to search for.
     * @returns The matching DatasetRow if found, otherwise undefined.
     */
    async searchDatasetRowByPayload(id, rowIdValue) {
        try {
            const collectionName = await this.findCollectionName(id);
            console.log(`[searchDatasetRowByPayload] Resolved collection name: ${collectionName}`);
            console.log(`[searchDatasetRowByPayload] Qdrant URL: ${process.env.QDRANT_URL}`);
            // Construct the filter for rowId
            const filter = {
                must: [
                    {
                        key: "rowId",
                        match: {
                            value: rowIdValue,
                        },
                    },
                ],
            };
            // Since Qdrant's search requires a vector, use a dummy vector
            const dummyVector = new Array(this.vectorSize).fill(0); // Dummy vector
            console.log(`[searchDatasetRowByPayload] Using dummy vector with length ${dummyVector.length}`);
            const response = await this.qdrant.search(collectionName, {
                vector: dummyVector, // Assign directly as number[]
                limit: 1,
                with_payload: true,
                with_vector: false, // We don't need the vector in this case
                filter: filter,
            });
            console.log(`[searchDatasetRowByPayload] Received response: ${JSON.stringify(response).substring(0, 500)}...`);
            const searchResult = response;
            if (searchResult.length > 0) {
                const point = searchResult[0];
                const rowId = point.payload?.rowId;
                if (typeof rowId !== 'string') {
                    console.error(`[searchDatasetRowByPayload] Invalid rowId type: expected string, got ${typeof rowId}`);
                    return undefined;
                }
                return {
                    id: rowId, // Use the original rowId as string
                    data: Array.isArray(point.payload?.data) ? point.payload.data.map(String) : [String(point.payload?.data)],
                    embedding: Array.isArray(point.vector) ? point.vector : [],
                };
            }
            else {
                console.log(`[searchDatasetRowByPayload] No point found with rowId: ${rowIdValue}`);
            }
        }
        catch (error) {
            console.error(`[searchDatasetRowByPayload] Error searching for rowId: ${rowIdValue}`, error.response?.data || error.message);
        }
        return undefined;
    }
    /**
     * Get dataset row by rowId
     * Aligns with the DatasetProvider interface: (id: DatasetId, rowId: string) => Promise<DatasetRow | undefined>
     */
    async getDatasetRowById(id, rowId) {
        try {
            const collectionName = await this.findCollectionName(id);
            // Use the payload-based search method
            const row = await this.searchDatasetRowByPayload(id, rowId);
            return row;
        }
        catch (error) {
            console.error(`[getDatasetRowById] Error retrieving dataset row by ID: ${rowId}`, error.response?.data || error.message);
        }
        return undefined;
    }
    /**
     * Get dataset data (required by DatasetProvider interface)
     * Aligns with the DatasetProvider interface: (id: DatasetId, options?: SearchOptions) => Promise<Dataset>
     */
    async getDatasetData(id, options) {
        try {
            const collectionName = await this.findCollectionName(id);
            let rows = [];
            if (options?.vector && this.isValidVector(options.vector)) {
                // Perform KNN search
                const k = options.k || 10; // Default to top 10
                console.log(`[getDatasetData] Performing KNN search with k=${k} and vector length=${options.vector.length}`);
                rows = await this.performSearch(collectionName, options.vector, k);
                console.log(`[getDatasetData] KNN search returned ${rows.length} rows`);
            }
            else {
                // Perform scroll to retrieve all points
                const batchSize = 1000; // Define an appropriate batch size
                console.log(`[getDatasetData] Performing scroll retrieval with batchSize=${batchSize}`);
                rows = await this.performScroll(collectionName, batchSize);
                console.log(`[getDatasetData] Scroll retrieval returned ${rows.length} rows`);
            }
            console.log(`[getDatasetData] Returning Dataset with id=${id} and ${rows.length} rows`);
            return { id: id, rows };
        }
        catch (error) {
            console.error('[getDatasetData] Error in getDatasetData:', error.response?.data || error.message);
            throw error;
        }
    }
    /**
     * Put dataset metadata (required by DatasetProvider interface)
     * Aligns with the DatasetProvider interface: (metadata: DatasetMetadata) => Promise<void>
     */
    async putDatasetMetadata(metadata) {
        const { id: datasetId, projectId } = metadata;
        if (!projectId) {
            throw new Error('ProjectId is required in DatasetMetadata.');
        }
        const collectionName = this.getCollectionName(projectId, datasetId);
        // Check if a creation is already in progress for this collection
        if (QdrantDatasetProvider.collectionCreationLocks.has(collectionName)) {
            console.log(`[putDatasetMetadata] Collection '${collectionName}' is currently being created. Waiting for the existing creation process to complete.`);
            await QdrantDatasetProvider.collectionCreationLocks.get(collectionName);
            console.log(`[putDatasetMetadata] Collection '${collectionName}' has been created by another process.`);
            return;
        }
        // Define the creation promise and add it to the lock map
        const creationPromise = (async () => {
            try {
                // Attempt to retrieve the collection to check its existence
                await this.qdrant.getCollection(collectionName);
                // If retrieval is successful, the collection exists
                console.log(`[putDatasetMetadata] Collection '${collectionName}' already exists. Skipping creation.`);
            }
            catch (error) {
                // Determine if the error is due to the collection not existing
                const isCollectionNotFound = error.response?.data?.code === 'not_found' ||
                    error.message?.toLowerCase().includes('not found');
                if (isCollectionNotFound) {
                    try {
                        // Define the vector field as 'vector' and create the collection
                        await this.qdrant.createCollection(collectionName, {
                            vectors: {
                                [QdrantDatasetProvider.VECTOR_FIELD_NAME]: {
                                    size: this.vectorSize, // Set to 1536 as required
                                    distance: 'Cosine',
                                },
                            },
                            // Removed payload_schema as it's not recognized by Qdrant client
                        });
                        console.log(`[putDatasetMetadata] Collection '${collectionName}' created successfully with vector field '${QdrantDatasetProvider.VECTOR_FIELD_NAME}'.`);
                    }
                    catch (createError) {
                        // Handle the case where the collection was created between the check and the creation attempt
                        const isAlreadyExistsError = createError.response?.data?.code === 'collection_already_exists' ||
                            createError.message?.toLowerCase().includes('already exists');
                        if (isAlreadyExistsError) {
                            console.log(`[putDatasetMetadata] Collection '${collectionName}' was created by another process. Proceeding without creation.`);
                        }
                        else {
                            console.error('[putDatasetMetadata] Error creating collection:', createError.response?.data || createError.message);
                            throw createError;
                        }
                    }
                }
                else {
                    // If the error is not about the collection not existing, rethrow it
                    console.error('[putDatasetMetadata] Error retrieving collection:', error.response?.data || error.message);
                    throw error;
                }
            }
        })();
        // Add the creation promise to the lock map
        QdrantDatasetProvider.collectionCreationLocks.set(collectionName, creationPromise);
        try {
            // Await the creation process
            await creationPromise;
        }
        finally {
            // Remove the promise from the lock map regardless of success or failure
            QdrantDatasetProvider.collectionCreationLocks.delete(collectionName);
        }
    }
    /**
     * Put dataset row by rowId
     * Aligns with the DatasetProvider interface: (id: DatasetId, row: DatasetRow) => Promise<void>
     */
    async putDatasetRow(id, row) {
        try {
            const collectionName = await this.findCollectionName(id);
            console.log(`[putDatasetRow] Inserting row into dataset ${id}, collection ${collectionName}`, row);
            let vector = {};
            if (row.embedding && row.embedding.length === this.vectorSize) {
                vector = row.embedding;
                // Validate vector
                if (!this.isValidVector(vector)) {
                    throw new Error(`Invalid vector content for rowId '${row.id}'.`);
                }
            }
            else {
                console.warn(`Embedding undefined or invalid for rowId '${row.id}'. Using empty vector.`);
            }
            // Generate UUID from rowId
            const pointId = this.rowIdToUUID(row.id);
            // Log upsert details
            if (Array.isArray(vector)) {
                console.log(`[putDatasetRow] Upserting point ID '${pointId}' with vector: [${vector.slice(0, 5).join(', ')}...]`);
            }
            else {
                console.log(`[putDatasetRow] Upserting point ID '${pointId}' with empty vector.`);
            }
            // Prepare the point object
            const point = {
                id: pointId,
                payload: { rowId: row.id, data: row.data },
            };
            if (Array.isArray(vector)) {
                point.vector = vector;
            }
            else {
                point.vector = {}; // Empty object for points without vectors
            }
            // Upsert the point
            await this.qdrant.upsert(collectionName, {
                points: [point],
            });
            console.log(`[putDatasetRow] Successfully upserted point ID '${pointId}'`);
        }
        catch (error) {
            console.error('[putDatasetRow] Error in putDatasetRow:', error.response?.data || error.message);
            throw error;
        }
    }
    /**
     * Put dataset data
     * Aligns with the DatasetProvider interface: (id: DatasetId, data: Dataset) => Promise<void>
     */
    async putDatasetData(id, data) {
        try {
            const collectionName = await this.findCollectionName(id);
            console.log(`[putDatasetData] Inserting dataset into ${id}, collection ${collectionName}`, data);
            const points = data.rows.map((row) => {
                let vector = {};
                if (row.embedding && row.embedding.length === this.vectorSize) {
                    vector = row.embedding;
                    // Validate vector content
                    if (!this.isValidVector(vector)) {
                        throw new Error(`Invalid vector content for rowId '${row.id}'.`);
                    }
                }
                else {
                    console.warn(`Embedding undefined or invalid for rowId '${row.id}'. Using empty vector.`);
                }
                // Generate UUID from rowId
                const pointId = this.rowIdToUUID(row.id);
                // Log upsert details
                if (Array.isArray(vector)) {
                    console.log(`[putDatasetData] Upserting point ID '${pointId}' with vector: [${vector.slice(0, 5).join(', ')}...]`);
                }
                else {
                    console.log(`[putDatasetData] Upserting point ID '${pointId}' with empty vector.`);
                }
                // Prepare the point object
                const point = {
                    id: pointId,
                    payload: { rowId: row.id, data: row.data },
                };
                if (Array.isArray(vector)) {
                    point.vector = vector;
                }
                else {
                    point.vector = {}; // Empty object for points without vectors
                }
                return point;
            });
            // Upsert points in batches if necessary
            const batchSize = 1000;
            for (let i = 0; i < points.length; i += batchSize) {
                const batch = points.slice(i, i + batchSize);
                console.log(`[putDatasetData] Upserting batch ${Math.floor(i / batchSize) + 1} with ${batch.length} points`);
                await this.qdrant.upsert(collectionName, { points: batch });
                console.log(`[putDatasetData] Successfully upserted batch ${Math.floor(i / batchSize) + 1}`);
            }
            console.log(`[putDatasetData] Dataset with ID '${id}' inserted successfully.`);
        }
        catch (error) {
            console.error('[putDatasetData] Error in putDatasetData:', error.response?.data || error.message);
            throw error;
        }
    }
    /**
     * Get K-nearest neighbors by vector
     * Aligns with the DatasetProvider interface: (id: DatasetId, k: number, vector: number[]) => Promise<(DatasetRow & { distance?: number })[]>
     */
    async knnDatasetRows(id, k, vector) {
        try {
            const collectionName = await this.findCollectionName(id);
            console.log(`[knnDatasetRows] KNN query on dataset ${id}, collection ${collectionName}, with vector length=${vector.length}`);
            console.log(`[knnDatasetRows] Performing KNN search with vector: [${vector.slice(0, 5).join(', ')}...]`);
            // Validate the input vector
            if (!this.isValidVector(vector)) {
                throw new Error(`Invalid query vector. Expected size ${this.vectorSize}.`);
            }
            const response = await this.qdrant.search(collectionName, {
                vector: vector, // Assign directly as number[]
                limit: k,
                with_payload: true,
                with_vector: true,
            });
            console.log(`[knnDatasetRows] Received response: ${JSON.stringify(response).substring(0, 500)}...`);
            const searchResult = response;
            const results = searchResult.map((result) => {
                const rowId = result.payload?.rowId;
                if (typeof rowId !== 'string') {
                    throw new Error(`Invalid rowId type: expected string, got ${typeof rowId}`);
                }
                return {
                    id: rowId, // Original rowId as string
                    data: Array.isArray(result.payload?.data)
                        ? result.payload.data.map(String)
                        : [String(result.payload?.data)],
                    embedding: Array.isArray(result.vector) ? result.vector : [],
                    distance: result.score, // Optional distance
                };
            });
            console.log(`[knnDatasetRows] Processed ${results.length} KNN results`);
            return results;
        }
        catch (error) {
            console.error('[knnDatasetRows] Error in knnDatasetRows:', error.response?.data || error.message);
            throw error;
        }
    }
    /**
     * Get dataset metadata
     * Aligns with the DatasetProvider interface: (id: DatasetId) => Promise<DatasetMetadata | undefined>
     */
    async getDatasetMetadata(id) {
        try {
            const collectionName = await this.findCollectionName(id);
            console.log(`[getDatasetMetadata] Fetching metadata for dataset ${id}, collection ${collectionName}`);
            const response = await this.qdrant.getCollection(collectionName);
            console.log(`[getDatasetMetadata] getCollection response: ${JSON.stringify(response, null, 2)}`);
            const projectId = this.getProjectIdFromCollectionName(collectionName);
            return {
                id: id,
                name: id,
                description: "", // Set to empty string or use another relevant property
                projectId: projectId,
            };
        }
        catch (error) {
            console.error(`[getDatasetMetadata] Error retrieving metadata for dataset ${id}`, error.response?.data || error.message);
            return undefined;
        }
    }
    /**
     * Clear dataset data
     * Aligns with the DatasetProvider interface: (id: DatasetId) => Promise<void>
     */
    async clearDatasetData(id) {
        try {
            const collectionName = await this.findCollectionName(id);
            console.log(`[clearDatasetData] Clearing dataset ${id}, collection ${collectionName}`);
            await this.qdrant.delete(collectionName, {
                filter: {}, // Use an empty filter to delete all points in the collection
            });
            console.log(`[clearDatasetData] All points in collection '${collectionName}' have been deleted.`);
        }
        catch (error) {
            console.error('[clearDatasetData] Error in clearDatasetData:', error.response?.data || error.message);
            throw error;
        }
    }
    /**
     * Delete dataset
     * Aligns with the DatasetProvider interface: (id: DatasetId) => Promise<void>
     */
    async deleteDataset(id) {
        try {
            const collectionName = await this.findCollectionName(id);
            console.log(`[deleteDataset] Deleting dataset ${id}, collection ${collectionName}`);
            await this.qdrant.deleteCollection(collectionName);
            console.log(`[deleteDataset] Deleted dataset collection '${collectionName}'.`);
        }
        catch (error) {
            console.error('[deleteDataset] Error in deleteDataset:', error.response?.data || error.message);
            throw error;
        }
    }
    /**
     * Get datasets for project
     * Aligns with the DatasetProvider interface: (projectId: ProjectId) => Promise<DatasetMetadata[]>
     */
    async getDatasetsForProject(projectId) {
        try {
            const prefix = `${projectId}.`;
            console.log(`[getDatasetsForProject] Fetching datasets for project ${projectId}`);
            const response = await this.qdrant.getCollections();
            const filteredCollections = response.collections.filter((collection) => collection.name.startsWith(prefix));
            return filteredCollections.map((collection) => {
                const datasetId = collection.name.substring(prefix.length);
                return {
                    id: datasetId,
                    name: datasetId,
                    description: "", // Set to empty string or use another relevant property
                    projectId: projectId,
                };
            });
        }
        catch (error) {
            console.error('[getDatasetsForProject] Error in getDatasetsForProject:', error.response?.data || error.message);
            return [];
        }
    }
    /**
     * Export datasets for project
     * Aligns with the DatasetProvider interface: (projectId: ProjectId) => Promise<CombinedDataset[]>
     */
    async exportDatasetsForProject(projectId) {
        try {
            const datasets = await this.getDatasetsForProject(projectId);
            const combinedDatasets = [];
            for (const metadata of datasets) {
                const data = await this.getDatasetData(metadata.id);
                combinedDatasets.push({
                    meta: metadata,
                    data,
                });
            }
            return combinedDatasets;
        }
        catch (error) {
            console.error('[exportDatasetsForProject] Error in exportDatasetsForProject:', error.response?.data || error.message);
            return [];
        }
    }
}
QdrantDatasetProvider.UUID_NAMESPACE = '6ba7b810-9dad-11d1-80b4-00c04fd430c8'; // DNS namespace UUID or any other
QdrantDatasetProvider.VECTOR_FIELD_NAME = 'vector';
/**
 * Static map to track ongoing collection creation promises.
 * Key: Collection name
 * Value: Promise representing the creation process
 */
QdrantDatasetProvider.collectionCreationLocks = new Map();
//# sourceMappingURL=QdrantDatasetProvider.js.map