import {
	IExecuteFunctions,
	INodeExecutionData,
	INodeType,
	INodeTypeDescription,
	NodeOperationError,
} from 'n8n-workflow';

export class LeadScraper implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'Lead Scraper',
		name: 'leadScraper',
		icon: 'file:leadScraper.svg',
		group: ['transform'],
		version: 1,
		subtitle: '={{$parameter["operation"]}}',
		description: 'Scrape business websites for leads — emails, phones, addresses, and more',
		defaults: {
			name: 'Lead Scraper',
		},
		inputs: ['main'],
		outputs: ['main'],
		credentials: [
			{
				name: 'leadScraperApi',
				required: true,
			},
		],
		properties: [
			// Operation
			{
				displayName: 'Operation',
				name: 'operation',
				type: 'options',
				noDataExpression: true,
				options: [
					{
						name: 'Search & Scrape',
						value: 'search',
						description: 'Search for businesses by keyword + cities, then scrape their websites',
						action: 'Search and scrape businesses',
					},
					{
						name: 'Scrape URL',
						value: 'scrapeUrl',
						description: 'Scrape a specific URL for contact data',
						action: 'Scrape a URL for contact data',
					},
				],
				default: 'search',
			},

			// Search fields
			{
				displayName: 'Keyword',
				name: 'keyword',
				type: 'string',
				default: '',
				placeholder: 'e.g. hvac, dentist, plumber',
				description: 'Business type to search for',
				displayOptions: {
					show: { operation: ['search'] },
				},
				required: true,
			},
			{
				displayName: 'Cities',
				name: 'cities',
				type: 'string',
				default: '',
				placeholder: 'e.g. Denver, Phoenix, Dallas',
				description: 'Comma-separated list of cities to search in',
				displayOptions: {
					show: { operation: ['search'] },
				},
				required: true,
			},
			{
				displayName: 'Max Leads',
				name: 'maxLeads',
				type: 'number',
				default: 50,
				description: 'Maximum number of leads to collect',
				displayOptions: {
					show: { operation: ['search'] },
				},
			},

			// Scrape URL fields
			{
				displayName: 'URL',
				name: 'url',
				type: 'string',
				default: '',
				placeholder: 'https://example.com',
				description: 'The URL to scrape for contact data',
				displayOptions: {
					show: { operation: ['scrapeUrl'] },
				},
				required: true,
			},

			// Advanced options
			{
				displayName: 'Options',
				name: 'options',
				type: 'collection',
				placeholder: 'Add Option',
				default: {},
				options: [
					{
						displayName: 'Stealth Mode',
						name: 'stealth',
						type: 'boolean',
						default: true,
						description: 'Whether to randomize browser fingerprint to avoid detection',
					},
					{
						displayName: 'Google Maps Search',
						name: 'googleMaps',
						type: 'boolean',
						default: false,
						description: 'Whether to also search Google Maps for business listings',
					},
					{
						displayName: 'Deep Crawl',
						name: 'deepCrawl',
						type: 'boolean',
						default: false,
						description: 'Whether to follow internal links to find more contact data',
					},
					{
						displayName: 'Concurrency',
						name: 'concurrency',
						type: 'number',
						default: 3,
						description: 'Number of parallel scrapers',
					},
					{
						displayName: 'Proxies',
						name: 'proxies',
						type: 'string',
						default: '',
						placeholder: 'http://proxy1:8080\\nhttp://proxy2:8080',
						description: 'Newline-separated list of proxy servers',
						typeOptions: {
							rows: 3,
						},
					},
					{
						displayName: 'Poll Interval (ms)',
						name: 'pollInterval',
						type: 'number',
						default: 3000,
						description: 'How often to check for job completion (milliseconds)',
					},
					{
						displayName: 'Timeout (s)',
						name: 'timeout',
						type: 'number',
						default: 600,
						description: 'Maximum time to wait for job completion (seconds)',
					},
				],
			},
		],
	};

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
		const items = this.getInputData();
		const returnData: INodeExecutionData[] = [];

		const credentials = await this.getCredentials('leadScraperApi');
		const baseUrl = (credentials.baseUrl as string).replace(/\/$/, '');

		for (let i = 0; i < items.length; i++) {
			try {
				const operation = this.getNodeParameter('operation', i) as string;
				const options = this.getNodeParameter('options', i, {}) as {
					stealth?: boolean;
					googleMaps?: boolean;
					deepCrawl?: boolean;
					concurrency?: number;
					proxies?: string;
					pollInterval?: number;
					timeout?: number;
				};

				let endpoint: string;
				let body: Record<string, unknown>;

				if (operation === 'search') {
					const keyword = this.getNodeParameter('keyword', i) as string;
					const cities = this.getNodeParameter('cities', i) as string;
					const maxLeads = this.getNodeParameter('maxLeads', i) as number;

					endpoint = `${baseUrl}/api/search`;
					body = {
						keyword,
						cities,
						num: maxLeads,
						stealth: options.stealth ?? true,
						google_maps: options.googleMaps ?? false,
						deep_crawl: options.deepCrawl ?? false,
						concurrency: options.concurrency ?? 3,
						proxies: options.proxies ?? '',
					};
				} else {
					const url = this.getNodeParameter('url', i) as string;

					endpoint = `${baseUrl}/api/scrape`;
					body = {
						url,
						stealth: options.stealth ?? true,
						deep_crawl: options.deepCrawl ?? false,
						concurrency: options.concurrency ?? 3,
						proxies: options.proxies ?? '',
					};
				}

				// Start the job
				const startResponse = await this.helpers.httpRequest({
					method: 'POST',
					url: endpoint,
					body,
					json: true,
				});

				if (startResponse.error) {
					throw new NodeOperationError(this.getNode(), startResponse.error, { itemIndex: i });
				}

				const jobId = startResponse.job_id;
				const pollInterval = options.pollInterval ?? 3000;
				const timeout = (options.timeout ?? 600) * 1000;
				const startTime = Date.now();

				// Poll for completion
				let status = 'pending';
				while (!['done', 'error', 'cancelled'].includes(status)) {
					if (Date.now() - startTime > timeout) {
						// Cancel the job on timeout
						await this.helpers.httpRequest({
							method: 'POST',
							url: `${baseUrl}/api/cancel/${jobId}`,
							json: true,
						});
						throw new NodeOperationError(
							this.getNode(),
							`Job timed out after ${options.timeout ?? 600}s`,
							{ itemIndex: i },
						);
					}

					await new Promise((resolve) => setTimeout(resolve, pollInterval));

					const progressResponse = await this.helpers.httpRequest({
						method: 'GET',
						url: `${baseUrl}/api/results/${jobId}`,
						json: true,
					});

					status = progressResponse.status;

					if (status === 'error') {
						throw new NodeOperationError(
							this.getNode(),
							'Scraping job failed',
							{ itemIndex: i },
						);
					}
				}

				// Fetch final results
				const resultsResponse = await this.helpers.httpRequest({
					method: 'GET',
					url: `${baseUrl}/api/results/${jobId}`,
					json: true,
				});

				const leads = resultsResponse.leads || [];

				// Output each lead as a separate item
				for (const lead of leads) {
					if (!lead.error) {
						returnData.push({
							json: {
								url: lead.url || '',
								company: lead.company || '',
								description: lead.description || '',
								emails: lead.emails || [],
								phones: lead.phones || [],
								address: lead.address || '',
								hours: lead.hours || '',
								socials: lead.socials || {},
								_jobId: jobId,
							},
						});
					}
				}

				// If no results, output empty item with metadata
				if (leads.length === 0) {
					returnData.push({
						json: {
							_jobId: jobId,
							_status: 'no_results',
							_message: 'No leads found',
						},
					});
				}
			} catch (error) {
				if (this.continueOnFail()) {
					returnData.push({
						json: { error: (error as Error).message },
					});
					continue;
				}
				throw error;
			}
		}

		return [returnData];
	}
}
