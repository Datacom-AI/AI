import mongoose, { Document, Schema } from 'mongoose';

export interface ProductData {
  barcode: any;
  origin: any;
  productName?: string;
  name?: string; // Alias for productName for compatibility
  brand?: string;
  manufacturer?: string; // Alias for brand for compatibility
  price?: number;
  currency?: string;
  description?: string;
  ingredients?: string[];
  nutritionFacts?: Record<string, string>;
  images?: string[];
  url?: string;
  sku?: string;
  category?: string;
  size?: string;
  metadata?: Record<string, any>;
}

export interface AIAnalysis {
  categories?: string[];
  keywords?: string[];
  sentiment?: number;
  confidenceScore?: number;
  summary?: string;
}

export interface CrawlResultDocument extends Document {
  taskId: Schema.Types.ObjectId;
  status: string;
  rawHtml?: string;
  extractedContent?: string;
  processedData: ProductData;
  aiAnalysis?: AIAnalysis;
  createdAt: Date;
  updatedAt: Date;
}

const crawlResultSchema = new Schema<CrawlResultDocument>(
  {
    taskId: {
      type: Schema.Types.ObjectId,
      ref: 'CrawlTask',
      required: true,
    },
    status: {
      type: String,
      enum: ['pending', 'processing', 'completed', 'failed'],
      default: 'pending',
    },
    rawHtml: {
      type: String,
    },
    extractedContent: {
      type: String,
    },
    processedData: {
      productName: String,
      brand: String,
      price: Number,
      description: String,
      ingredients: [String],
      nutritionFacts: {
        type: Map,
        of: String,
      },
      images: [String],
      url: String,
      metadata: {
        type: Map,
        of: Schema.Types.Mixed,
      },
    },
    aiAnalysis: {
      categories: [String],
      keywords: [String],
      sentiment: Number,
      confidenceScore: Number,
      summary: String,
    },
  },
  {
    timestamps: true,
  }
);

export default mongoose.model<CrawlResultDocument>('CrawlResult', crawlResultSchema); 