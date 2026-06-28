import { Injectable, NotFoundException } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { LessThan, type Repository } from 'typeorm';
import { Message } from '../entities/message.entity';
import { CreateMessageDto } from './dto/create-message.dto';
import { UpdateMessageDto } from './dto/update-message.dto';

@Injectable()
export class MessagesService {
  constructor(
    @InjectRepository(Message)
    private readonly messageRepository: Repository<Message>,
  ) {}

  async create(createMessageDto: CreateMessageDto): Promise<Message> {
    const message = this.messageRepository.create(createMessageDto);
    return this.messageRepository.save(message);
  }

  /**
   * Production-readiness fix: bounded result set.
   *
   * Previously called `messageRepository.find({ order: ... })` with no
   * `take`/`skip` at all — every call returned the ENTIRE table, with no
   * limit, and (before migration 008) no supporting index on `createdAt`
   * either. At low row counts this was invisible; at production scale,
   * every call was both an unbounded-size response payload and, combined
   * with the missing index, a full table scan plus an in-memory sort on
   * every invocation.
   *
   * `limit` is optional with a sane default (50) and a HARD ceiling
   * (200) enforced regardless of what the caller requests — `Math.min`
   * here, not a validation error, because a caller passing an
   * out-of-range value almost certainly just wants "as many as
   * reasonable", not a rejected request.
   *
   * `cursor` (an ISO-8601 timestamp) is cursor-based, not offset-based,
   * pagination — deliberately, since offset pagination
   * (`skip: pageNumber * pageSize`) degrades as the table grows AND
   * shifts under concurrent inserts (a row inserted between two page
   * fetches shifts every subsequent offset by one). Cursor pagination on
   * the now-indexed `createdAt` column (migration 008) avoids both
   * problems: `WHERE "createdAt" < :cursor ORDER BY "createdAt" DESC`
   * is a stable, indexed range scan regardless of how many rows exist.
   */
  async findAll(
    opts: { limit?: number; cursor?: string } = {},
  ): Promise<Message[]> {
    const limit = Math.min(opts.limit ?? 50, 200);

    if (opts.cursor) {
      return this.messageRepository.find({
        where: { createdAt: LessThan(new Date(opts.cursor)) },
        order: { createdAt: 'DESC' },
        take: limit,
      });
    }

    return this.messageRepository.find({
      order: { createdAt: 'DESC' },
      take: limit,
    });
  }

  async findOne(id: number): Promise<Message> {
    const message = await this.messageRepository.findOne({ where: { id } });
    if (!message) {
      throw new NotFoundException(`Message with id ${id} not found`);
    }
    return message;
  }

  async update(
    id: number,
    updateMessageDto: UpdateMessageDto,
  ): Promise<Message> {
    const message = await this.findOne(id);
    Object.assign(message, updateMessageDto);
    return this.messageRepository.save(message);
  }

  async remove(id: number): Promise<void> {
    const message = await this.findOne(id);
    await this.messageRepository.remove(message);
  }
}
