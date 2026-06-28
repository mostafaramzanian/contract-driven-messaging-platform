import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { TopologyService } from './topology.service';
import { RetryPublisherService } from './retry-publisher.service';
import { DlqConsumerService } from './dlq-consumer.service';

@Module({
  imports: [ConfigModule],
  providers: [TopologyService, RetryPublisherService, DlqConsumerService],
  exports: [RetryPublisherService],
})
export class ReliabilityModule {}
