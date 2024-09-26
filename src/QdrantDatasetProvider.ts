import { QdrantClient } from '@qdrant/js-client-rest';
import {
  Dataset,
  DatasetRow,
  DatasetMetadata,
  DatasetProvider,
  DatasetId,
  ProjectId,
  CombinedDataset,
} from '@ironclad/rivet-core';
import dotenv from 'dotenv';
import { v5 as uuidv5 } from 'uuid';
dotenv.config();

// Define interfaces for payload and search response
interface PointPayload {
  rowId: string;
  data: string[];
}

interface QdrantSearchResult {
  id: string | number;
  score?: number;
  payload?: PointPayload;
  vector?: number[] | null;
}

interface CollectionInfo {
  name: string;
  vectors?: {
    size: number;
    distance: string;
  };
  // Add other properties as needed
}

interface GetCollectionsResponse {
  collections: CollectionInfo[];
}

interface GetCollectionResponse {
  status: 'red' | 'green' | 'yellow' | 'grey';
  optimizer_status: 'ok' | { error: string };
  vectors_count?: number;
  indexed_vectors_count?: number;
  points_count?: number;
  segments_count: number;
  config: {
    // Define relevant properties
    [key: string]: any;
  };
  // Removed payload_schema as it's not recognized by Qdrant client
}

export class QdrantDatasetProvider implements DatasetProvider {
  private qdrant: QdrantClient;
  private vectorSize: number;
  private static readonly UUID_NAMESPACE = '6ba7b810-9dad-11d1-80b4-00c04fd430c8'; // DNS namespace UUID or any other

  constructor(qdrantUrl: string, vectorSize: number = 1536, apiKey?: string) {
    this.qdrant = new QdrantClient({
      url: qdrantUrl,
      apiKey, // Include API Key if provided
    });
    this.vectorSize = vectorSize;
  }

  /**
   * Factory method to create an instance of QdrantDatasetProvider
   */
  public static async create(): Promise<QdrantDatasetProvider> {
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
  private rowIdToUUID(rowId: string): string {
    return uuidv5(rowId, QdrantDatasetProvider.UUID_NAMESPACE);
  }

  /**
   * Converts a UUID back to the original rowId.
   * Note: UUID v5 is a one-way function; to retrieve the original rowId,
   * it must be stored in the payload.
   * @param uuid - The UUID string.
   * @returns The original rowId string.
   */
  private uuidToRowId(uuid: string): string | undefined {
    // Since UUID v5 is one-way, retrieve rowId from payload during read operations.
    // This function can be used if reverse mapping is needed based on application logic.
    return undefined; // Placeholder
  }

  /**
   * Constructs the collection name based on projectId and datasetId.
   * Format: <projectId>.<datasetId>
   */
  private getCollectionName(projectId: string, datasetId: string): string {
    return `${projectId}.${datasetId}`;
  }

  /**
   * Extracts the projectId from the collection name.
   * @param collectionName - The name of the collection in the format <projectId>.<datasetId>.
   * @returns The extracted ProjectId.
   */
  private getProjectIdFromCollectionName(collectionName: string): string {
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
  private async findCollectionName(datasetId: string): Promise<string> {
    const response: GetCollectionsResponse = await this.qdrant.getCollections();
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
  private isValidVector(vector: number[]): boolean {
    return (
      Array.isArray(vector) &&
      vector.length === this.vectorSize &&
      vector.every(num => typeof num === 'number')
    );
  }

  /**
   * Performs a KNN search on the specified collection.
   * @param collectionName - The name of the collection to search.
   * @param vector - The query vector for similarity.
   * @param k - The number of nearest neighbors to retrieve.
   * @returns An array of DatasetRows matching the search criteria.
   */
  private async performSearch(collectionName: string, vector: number[], k: number): Promise<DatasetRow[]> {
    //console.log(`[performSearch] Starting KNN search on collection: ${collectionName} with k=${k}`);
    //console.log(`[performSearch] Query Vector (first 10 elements): ${JSON.stringify(vector.slice(0, 10))}...`); // Log first 10 elements for brevity

    try {
      const response = await this.qdrant.search(collectionName, {
        vector: vector,
        limit: k,
        with_payload: true,
        with_vector: true,
      });

      // Access the 'result' array
      const searchResult: QdrantSearchResult[] = response as unknown as QdrantSearchResult[];

      //console.log(`[performSearch] Search returned ${searchResult.length} points`);

      const processedResults: DatasetRow[] = searchResult.map((res) => {
        const rowId = res.payload?.rowId;
        if (typeof rowId !== 'string') {
          throw new Error(`Invalid rowId type: expected string, got ${typeof rowId}`);
        }

        return {
          id: rowId, // Use the original rowId as string
          data: Array.isArray(res.payload?.data) ? res.payload.data.map(String) : [String(res.payload?.data)],
          embedding: Array.isArray(res.vector) ? (res.vector as number[]) : [],
          // 'distance' is optional and not part of DatasetRow, but included if needed elsewhere
          // You can choose to omit it or extend DatasetRow to include it
        };
      });

      //console.log(`[performSearch] Processed ${processedResults.length} DatasetRows`);
      return processedResults;
    } catch (error) {
      //console.error(`[performSearch] Error during KNN search:`, error);
      throw error;
    }
  }

  /**
   * Retrieves points from the specified collection using the scroll method.
   * @param collectionName - The name of the collection to retrieve points from.
   * @param batchSize - The number of points to retrieve per scroll.
   * @returns An array of DatasetRows retrieved from the collection.
   */
  private async performScroll(collectionName: string, batchSize: number): Promise<DatasetRow[]> {
    //console.log(`[performScroll] Starting scroll retrieval on collection: ${collectionName} with batchSize=${batchSize}`);

    try {
      const scrollResult = await this.qdrant.scroll(collectionName, {
        limit: batchSize,
        filter: {}, // Define filters if needed
      });

      //console.log(`[performScroll] Scroll returned ${scrollResult.points.length} points`);

      const processedResults: DatasetRow[] = scrollResult.points.map((point) => {
        const rowId = point.payload?.rowId;
        if (typeof rowId !== 'string') {
          throw new Error(`Invalid rowId type: expected string, got ${typeof rowId}`);
        }

        return {
          id: rowId,
          data: Array.isArray(point.payload?.data) ? point.payload.data.map(String) : [String(point.payload?.data)],
          embedding: Array.isArray(point.vector) ? (point.vector as number[]) : [],
        };
      });

      // Log each retrieved row's ID for verification
      processedResults.forEach((row, index) => {
        //console.log(`[performScroll] Retrieved Row ${index + 1}: ID=${row.id}`);
      });

      //console.log(`[performScroll] Processed ${processedResults.length} DatasetRows`);
      return processedResults;
    } catch (error) {
      console.error(`[performScroll] Error during scroll retrieval:`, error);
      throw error;
    }
  }

  /**
   * Retrieves a dataset row by searching for a specific payload field value.
   * @param id - The DatasetId.
   * @param rowIdValue - The rowId value to search for.
   * @returns The matching DatasetRow if found, otherwise undefined.
   */
  async searchDatasetRowByPayload(
    id: string,
    rowIdValue: string
  ): Promise<DatasetRow | undefined> {
    //console.log(`\n[searchDatasetRowByPayload] Called with DatasetId: ${id} and rowIdValue: ${rowIdValue}`);

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

      //console.log(`[searchDatasetRowByPayload] Performing filter-based search with filter: ${JSON.stringify(filter, null, 2)}`);

      // Since Qdrant's search requires a vector, we'll use a dummy vector.
      // Alternatively, consider using a KNN search with a minimal vector.
      const dummyVector = new Array<number>(this.vectorSize).fill(0); // Dummy vector

      const response = await this.qdrant.search(collectionName, {
        vector: dummyVector,
        limit: 1,
        with_payload: true,
        with_vector: false, // We don't need the vector in this case
        filter: filter,
      });

      // Access the 'result' array
      const searchResult: QdrantSearchResult[] = response as unknown as QdrantSearchResult[];

      //console.log(`[searchDatasetRowByPayload] Search returned ${searchResult.length} points`);

      if (searchResult.length > 0) {
        const point = searchResult[0];
        //console.log(`[searchDatasetRowByPayload] Found point: ${JSON.stringify(point, null, 2)}`);

        const rowId = point.payload?.rowId;
        if (typeof rowId !== 'string') {
          console.error(`[searchDatasetRowByPayload] Invalid rowId type: expected string, got ${typeof rowId}`);
          return undefined;
        }

        return {
          id: rowId, // Use the original rowId as string
          data: Array.isArray(point.payload?.data) ? point.payload.data.map(String) : [String(point.payload?.data)],
          embedding: Array.isArray(point.vector) ? (point.vector as number[]) : [],
        };
      } else {
        //console.log(`[searchDatasetRowByPayload] No point found with rowId: ${rowIdValue}`);
      }
    } catch (error) {
      console.error(`[searchDatasetRowByPayload] Error searching for rowId: ${rowIdValue}`, error);
    }

    return undefined;
  }

  /**
   * Get dataset row by rowId
   * Aligns with the DatasetProvider interface: (id: DatasetId, rowId: string) => Promise<DatasetRow | undefined>
   */
  async getDatasetRowById(
    id: string,
    rowId: string
  ): Promise<DatasetRow | undefined> {
    //console.log(`\n[getDatasetRowById] Called with DatasetId: ${id} and rowId: ${rowId}`);

    try {
      const collectionName = await this.findCollectionName(id);
      //console.log(`[getDatasetRowById] Resolved collection name: ${collectionName}`);
      //console.log(`[getDatasetRowById] Qdrant URL: ${process.env.QDRANT_URL}`);

      // Use the payload-based search method
      const row = await this.searchDatasetRowByPayload(id, rowId);
      //console.log(`[getDatasetRowById] Retrieved row: ${JSON.stringify(row, null, 2)}`);

      return row;
    } catch (error) {
      console.error(`[getDatasetRowById] Error retrieving dataset row by ID: ${rowId}`, error);
    }

    return undefined;
  }

  /**
   * Get dataset data (required by DatasetProvider interface)
   * Aligns with the DatasetProvider interface: (id: DatasetId, options?: SearchOptions) => Promise<Dataset>
   */
  async getDatasetData(id: string, options?: { vector?: number[]; k?: number }): Promise<Dataset> {
    //console.log(`\n[getDatasetData] Called with DatasetId: ${id} and options: ${JSON.stringify(options)}`);

    try {
      const collectionName = await this.findCollectionName(id);
      //console.log(`[getDatasetData] Resolved collection name: ${collectionName}`);
      //console.log(`[getDatasetData] Qdrant URL: ${process.env.QDRANT_URL}`);

      let rows: DatasetRow[] = [];

      if (options?.vector && this.isValidVector(options.vector)) {
        // Perform KNN search
        const k = options.k || 10; // Default to top 10
        //console.log(`[getDatasetData] Performing KNN search with k=${k} and vector length=${options.vector.length}`);

        rows = await this.performSearch(collectionName, options.vector, k);
        //console.log(`[getDatasetData] KNN search returned ${rows.length} rows`);
      } else {
        // Perform scroll to retrieve all points
        const batchSize = 1000; // Define an appropriate batch size
        //console.log(`[getDatasetData] Performing scroll retrieval with batchSize=${batchSize}`);

        rows = await this.performScroll(collectionName, batchSize);
        //console.log(`[getDatasetData] Scroll retrieval returned ${rows.length} rows`);
      }

      //console.log(`[getDatasetData] Returning Dataset with id=${id} and ${rows.length} rows`);
      return { id: id as DatasetId, rows };
    } catch (error) {
      console.error('[getDatasetData] Error in getDatasetData:', error);
      throw error;
    }
  }

  /**
   * Put dataset metadata (required by DatasetProvider interface)
   * Aligns with the DatasetProvider interface: (metadata: DatasetMetadata) => Promise<void>
   */
  async putDatasetMetadata(metadata: DatasetMetadata): Promise<void> {
    try {
      const { id: datasetId, projectId } = metadata;
      if (!projectId) {
        throw new Error('ProjectId is required in DatasetMetadata.');
      }

      const collectionName = this.getCollectionName(projectId, datasetId);
      //console.log(`[putDatasetMetadata] Creating collection with name: ${collectionName}`);

      await this.qdrant.createCollection(collectionName, {
        vectors: {
          size: this.vectorSize, // Set to 1536 as required
          distance: 'Cosine',
        },
        // Removed payload_schema as it's not recognized by Qdrant client
      });

      //console.log(`[putDatasetMetadata] Collection '${collectionName}' created successfully.`);
    } catch (error) {
      console.error('[putDatasetMetadata] Error in putDatasetMetadata:', error);
      throw error;
    }
  }

  /**
   * Put dataset row by rowId
   * Aligns with the DatasetProvider interface: (id: DatasetId, row: DatasetRow) => Promise<void>
   */
  async putDatasetRow(id: string, row: DatasetRow): Promise<void> {
    try {
      const collectionName = await this.findCollectionName(id);
      //console.log(`[putDatasetRow] Inserting row into dataset ${id}, collection ${collectionName}`, row);

      // Ensure vector size is 1536
      const vector = row.embedding && row.embedding.length === this.vectorSize
        ? row.embedding
        : new Array<number>(this.vectorSize).fill(0);

      // Generate UUID from rowId
      const pointId = this.rowIdToUUID(row.id);

      // Use rowId as a payload field for retrieval
      await this.qdrant.upsert(collectionName, {
        points: [
          {
            id: pointId, // UUID as point ID
            vector, // Correct vector size
            payload: { rowId: row.id, data: row.data },
          },
        ],
      });

      //console.log(`[putDatasetRow] Row with ID '${row.id}' inserted successfully with point ID '${pointId}'.`);
    } catch (error) {
      console.error('[putDatasetRow] Error in putDatasetRow:', error);
      throw error;
    }
  }

  /**
   * Put dataset data
   * Aligns with the DatasetProvider interface: (id: DatasetId, data: Dataset) => Promise<void>
   */
  async putDatasetData(id: string, data: Dataset): Promise<void> {
    try {
      const collectionName = await this.findCollectionName(id);
      //console.log(`[putDatasetData] Inserting dataset into ${id}, collection ${collectionName}`, data);

      const points = data.rows.map((row) => {
        // Ensure vector size is 1536
        const vector = row.embedding && row.embedding.length === this.vectorSize
          ? row.embedding
          : new Array<number>(this.vectorSize).fill(0);

        // Generate UUID from rowId
        const pointId = this.rowIdToUUID(row.id);

        // Validate rowId type
        if (typeof row.id !== 'string') {
          throw new Error(`Invalid rowId type: expected string, got ${typeof row.id}`);
        }

        return {
          id: pointId, // UUID as point ID
          vector, // Correct vector size
          payload: { rowId: row.id, data: row.data },
        };
      });

      await this.qdrant.upsert(collectionName, { points });
      //console.log(`[putDatasetData] Dataset with ID '${id}' inserted successfully.`);
    } catch (error) {
      console.error('[putDatasetData] Error in putDatasetData:', error);
      throw error;
    }
  }

  /**
   * Get K-nearest neighbors by vector
   * Aligns with the DatasetProvider interface: (id: DatasetId, k: number, vector: number[]) => Promise<(DatasetRow & { distance?: number })[]>
   */
  async knnDatasetRows(
    id: string,
    k: number,
    vector: number[]
  ): Promise<(DatasetRow & { distance?: number })[]> {
    try {
      const collectionName = await this.findCollectionName(id);
      //console.log(`[knnDatasetRows] KNN query on dataset ${id}, collection ${collectionName}, with vector: ${JSON.stringify(vector.slice(0, 10))}...`);

      const response = await this.qdrant.search(collectionName, {
        vector,
        limit: k,
        with_payload: true,
        with_vector: true,
      });

      // Access the 'result' array
      const searchResult: QdrantSearchResult[] = response as unknown as QdrantSearchResult[];

     // console.log(`[knnDatasetRows] KNN response: ${searchResult.length} points found`);

      const results: (DatasetRow & { distance?: number })[] = searchResult.map((result) => {
        const rowId = result.payload?.rowId;
        if (typeof rowId !== 'string') {
          throw new Error(`Invalid rowId type: expected string, got ${typeof rowId}`);
        }

        return {
          id: rowId, // Original rowId as string
          data: Array.isArray(result.payload?.data)
            ? result.payload.data.map(String)
            : [String(result.payload?.data)],
          embedding: Array.isArray(result.vector) ? (result.vector as number[]) : [],
          distance: result.score, // Optional distance
        };
      });

      return results;
    } catch (error) {
      console.error('[knnDatasetRows] Error in knnDatasetRows:', error);
      throw error;
    }
  }

  /**
   * Get dataset metadata
   * Aligns with the DatasetProvider interface: (id: DatasetId) => Promise<DatasetMetadata | undefined>
   */
  async getDatasetMetadata(id: string): Promise<DatasetMetadata | undefined> {
    try {
      const collectionName = await this.findCollectionName(id);
      //console.log(`[getDatasetMetadata] Fetching metadata for dataset ${id}, collection ${collectionName}`);

      const response: GetCollectionResponse = await this.qdrant.getCollection(collectionName);
      //console.log(`[getDatasetMetadata] getCollection response: ${JSON.stringify(response, null, 2)}`);

      const projectId = this.getProjectIdFromCollectionName(collectionName);

      return {
        id: id as DatasetId,
        name: id,
        description: "", // Set to empty string or use another relevant property
        projectId: projectId as ProjectId,
      };
    } catch (error) {
      console.error(`[getDatasetMetadata] Error retrieving metadata for dataset ${id}`, error);
      return undefined;
    }
  }

  /**
   * Clear dataset data
   * Aligns with the DatasetProvider interface: (id: DatasetId) => Promise<void>
   */
  async clearDatasetData(id: string): Promise<void> {
    try {
      const collectionName = await this.findCollectionName(id);
      //console.log(`[clearDatasetData] Clearing dataset ${id}, collection ${collectionName}`);

      await this.qdrant.delete(collectionName, {
        filter: {}, // Use an empty filter to delete all points in the collection
      });
      //console.log(`[clearDatasetData] All points in collection '${collectionName}' have been deleted.`);
    } catch (error) {
      console.error('[clearDatasetData] Error in clearDatasetData:', error);
      throw error;
    }
  }

  /**
   * Delete dataset
   * Aligns with the DatasetProvider interface: (id: DatasetId) => Promise<void>
   */
  async deleteDataset(id: string): Promise<void> {
    try {
      const collectionName = await this.findCollectionName(id);
      //console.log(`[deleteDataset] Deleting dataset ${id}, collection ${collectionName}`);

      await this.qdrant.deleteCollection(collectionName);
      //console.log(`[deleteDataset] Deleted dataset collection '${collectionName}'.`);
    } catch (error) {
      console.error('[deleteDataset] Error in deleteDataset:', error);
      throw error;
    }
  }

  /**
   * Get datasets for project
   * Aligns with the DatasetProvider interface: (projectId: ProjectId) => Promise<DatasetMetadata[]>
   */
  async getDatasetsForProject(projectId: string): Promise<DatasetMetadata[]> {
    try {
      const prefix = `${projectId}.`;
      //console.log(`[getDatasetsForProject] Fetching datasets for project ${projectId}`);
      const response: GetCollectionsResponse = await this.qdrant.getCollections();

      //console.log(`[getDatasetsForProject] Collections retrieved: ${JSON.stringify(response.collections, null, 2)}`);

      const filteredCollections = response.collections.filter((collection) =>
        collection.name.startsWith(prefix)
      );

      return filteredCollections.map((collection) => {
        const datasetId = collection.name.substring(prefix.length);
        return {
          id: datasetId as DatasetId,
          name: datasetId,
          description: "", // Set to empty string or use another relevant property
          projectId: projectId as ProjectId,
        };
      });
    } catch (error) {
      console.error('[getDatasetsForProject] Error in getDatasetsForProject:', error);
      return [];
    }
  }

  /**
   * Export datasets for project
   * Aligns with the DatasetProvider interface: (projectId: ProjectId) => Promise<CombinedDataset[]>
   */
  async exportDatasetsForProject(projectId: string): Promise<CombinedDataset[]> {
    try {
      const datasets = await this.getDatasetsForProject(projectId);
      const combinedDatasets: CombinedDataset[] = [];

      for (const metadata of datasets) {
        const data = await this.getDatasetData(metadata.id);
        combinedDatasets.push({
          meta: metadata,
          data,
        } as CombinedDataset);
      }

      return combinedDatasets;
    } catch (error) {
      console.error('[exportDatasetsForProject] Error in exportDatasetsForProject:', error);
      return [];
    }
  }
}