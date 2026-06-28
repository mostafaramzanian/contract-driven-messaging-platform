import { Controller } from '@nestjs/common';
import { MessagePattern, Payload } from '@nestjs/microservices';
import { MessagesService } from './messages.service';
import { CreateMessageDto } from './dto/create-message.dto';
import { UpdateMessageDto } from './dto/update-message.dto';
import type { Message } from '../entities/message.entity';

@Controller()
export class MessagesController {
  constructor(private readonly messagesService: MessagesService) {}

  @MessagePattern('messages.create')
  create(@Payload() createMessageDto: CreateMessageDto): Promise<Message> {
    return this.messagesService.create(createMessageDto);
  }

  @MessagePattern('findAllMessages')
  findAll(
    @Payload() query: { limit?: number; cursor?: string } = {},
  ): Promise<Message[]> {
    return this.messagesService.findAll(query);
  }

  @MessagePattern('findOneMessage')
  findOne(@Payload() id: number): Promise<Message> {
    return this.messagesService.findOne(id);
  }

  @MessagePattern('updateMessage')
  update(@Payload() updateMessageDto: UpdateMessageDto): Promise<Message> {
    return this.messagesService.update(updateMessageDto.id, updateMessageDto);
  }

  @MessagePattern('removeMessage')
  remove(@Payload() id: number): Promise<void> {
    return this.messagesService.remove(id);
  }
}
