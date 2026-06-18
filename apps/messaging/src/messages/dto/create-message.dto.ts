import { IsString, IsOptional, MinLength, MaxLength } from 'class-validator';

export class CreateMessageDto {
  @IsString()
  @MinLength(2)
  @MaxLength(255)
  title: string;

  @IsString()
  @MinLength(1)
  content: string;

  @IsString()
  @MinLength(2)
  @MaxLength(255)
  sender: string;

  @IsOptional()
  @IsString()
  @MaxLength(255)
  recipient?: string;
}
