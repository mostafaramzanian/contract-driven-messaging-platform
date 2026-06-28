import { Controller, Get, Res } from '@nestjs/common';
import type { Response } from 'express';
import { MetricsService } from './metrics.service';

/**
 * MetricsController
 *
 * Exposes Prometheus text-format metrics at GET /metrics.
 *
 *  - Gateway:   http://gateway:3005/metrics        (not behind Nginx)
 *  - Messaging: http://messaging:3006/metrics       (internal health port)
 *
 * Both ports are added to docker-compose networking and scraped directly
 * by Prometheus — see prometheus/prometheus.yml.
 */
@Controller('metrics')
export class MetricsController {
  constructor(private readonly metrics: MetricsService) {}

  @Get()
  async getMetrics(@Res() res: Response): Promise<void> {
    res.set('Content-Type', this.metrics.contentType);
    res.set('Cache-Control', 'no-cache');
    res.send(await this.metrics.metricsText());
  }
}
